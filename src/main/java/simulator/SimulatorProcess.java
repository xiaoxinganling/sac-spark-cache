package simulator;

import entity.Job;
import entity.RDD;
import entity.Stage;
import org.apache.log4j.Logger;
import utils.NumberUtil;

import java.io.*;
import java.util.*;

public class SimulatorProcess {

    private static Logger logger = Logger.getLogger(SimulatorProcess.class);

    public static long curJobId = -1;

    public static final String PARALLEL_INFO = "a_parallelism_of_stage_of_application";

    public static final String MEMORY_SIZE_INFO = "a_propose_memory_size_of_application";

    public static void processWithNoCache(String[] applicationNames, String[] fileNames) {
        // KEYPOINT: this function will record the max parallelism of different application, and the total memory size of hot data
//        StringBuilder sb = new StringBuilder();
        StageDispatcher sd = new StageDispatcher("NO_CACHE", 4);
        boolean parallelismHasRecorded = new File(PARALLEL_INFO).exists();
        boolean memorySizeHasRecorded = new File(MEMORY_SIZE_INFO).exists();
        List<Double> applicationTimeToPrint = new ArrayList<>();
        for(int i = 0; i < applicationNames.length; i++) {
//            if (!applicationNames[i].contains("spark_svm")) {
//                continue;
//            }
            int parallelism = Integer.MIN_VALUE;
            String application = applicationNames[i];
            String applicationFileName = fileNames[i];
            JobStageSubmitter jss = new JobStageSubmitter(application, applicationFileName);
            double applicationTotalTime = 0;
            for(Job job : jss.jobList) {
                double jobTotalTime = 0;
                List<Stage> tmp = jss.submitAvailableJob();
                // compute max
                parallelism = Math.max(parallelism, tmp.size());
                // end compute
                sd.dispatchStage(tmp);
                double curTime = sd.runStages();
                jobTotalTime += curTime;
//                sb.append(tmp.get(0).stageId).append(":").append(curTime).append("\n");
                List<Stage> toSubmit;
                while((toSubmit = jss.submitAvailableStages()) != null) {
                    // compute max
                    parallelism = Math.max(parallelism, toSubmit.size());
                    // end compute
                    sd.dispatchStage(toSubmit);
                    parallelism = Math.max(parallelism, toSubmit.size());
                    curTime = sd.runStages();
                    jobTotalTime += curTime;
//                    sb.append(toSubmit.get(0).stageId).append(":").append(curTime).append("\n");
                }
                logger.info(String.format("SimulatorProcess: application [%s] job [%s] has run for [%f]s.)",
                        application, job.jobId, jobTotalTime));
                applicationTotalTime += jobTotalTime;
            }
            logger.debug(String.format("SimulatorProcess: application [%s] has run for [%f]s.)",
                    application, applicationTotalTime));
            applicationTimeToPrint.add(applicationTotalTime);
            if (!parallelismHasRecorded) {
                try {
                    BufferedWriter bw = new BufferedWriter(new FileWriter(PARALLEL_INFO, true));
                    bw.write(parallelism + " ");
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (!memorySizeHasRecorded) {
                try {
                    List<RDD> hotData = HotDataGenerator.hotRDD(application, jss.jobList, null);
                    long proposedSize = HotDataGenerator.proposeCacheSpaceSize(application, hotData);
                    BufferedWriter bw = new BufferedWriter(new FileWriter(MEMORY_SIZE_INFO, true));
                    bw.write(proposedSize + " ");
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        for (int i = 0; i < applicationTimeToPrint.size(); i++) {
            System.out.println(applicationNames[i]);
        }
        System.out.println("=============================");
        for (double time : applicationTimeToPrint) {
            System.out.println(time / 1000);
        }
//        System.out.println(applicationTimeToPrint);
//        System.out.println(sb.toString());
    }

    public static void processWithInitialCache(String[] applicationNames, String[] fileNames) {
        CacheSpace cacheSpace = new CacheSpace(10, ReplacePolicy.FIFO);
        RDD tmpRDD = new RDD();
        tmpRDD.rddId = 38L;
        tmpRDD.partitionNum = 1L;
        cacheSpace.addRDD(tmpRDD);
        StageDispatcher sd = new StageDispatcher("INIT_CACHE", 4, cacheSpace);
        List<Double> applicationTimeToPrint = new ArrayList<>();
        for(int i = 0; i < applicationNames.length; i++) {
            String application = applicationNames[i];
            String applicationFileName = fileNames[i];
            JobStageSubmitter jss = new JobStageSubmitter(application, applicationFileName);
            double applicationTotalTime = 0;
            for(Job job : jss.jobList) {
                double jobTotalTime = 0;
                sd.dispatchStage(jss.submitAvailableJob());
                jobTotalTime += sd.runStagesWithCacheSpace();
                List<Stage> toSubmit;
                while((toSubmit = jss.submitAvailableStages()) != null) {
                    sd.dispatchStage(toSubmit);
                    jobTotalTime += sd.runStagesWithCacheSpace();
                }
                logger.info(String.format("SimulatorProcess: application [%s] job [%s] has run for [%f]s.)",
                        application, job.jobId, jobTotalTime));
                applicationTotalTime += jobTotalTime;
            }
            logger.debug(String.format("SimulatorProcess: application [%s] has run for [%f]s.)",
                    application, applicationTotalTime));
            applicationTimeToPrint.add(applicationTotalTime);
        }
        System.out.println(applicationTimeToPrint);
    }

    public static void writeExpStatisticsBatch(double[] cacheSpaceRatio, double[] parallelismRatio,
                                               ReplacePolicy[] replacePolicies, String runTimePath,
                                               String hitRatioPath, String[] applicationName,
                                               String[] applicationPath, boolean needHotData) throws IOException {
        if (new File(runTimePath).exists() || new File(hitRatioPath).exists()) {
            return;
        }
        BufferedWriter timeBw = new BufferedWriter(new FileWriter(runTimePath, true));
        BufferedWriter hitRatioBw = new BufferedWriter(new FileWriter(hitRatioPath, true));
        ArrayList<Integer> csSizes = new ArrayList<>();
        ArrayList<Integer> parallelisms = new ArrayList<>();
        BufferedReader csBr = new BufferedReader(new FileReader(SimulatorProcess.MEMORY_SIZE_INFO));
        BufferedReader parallelBr = new BufferedReader(new FileReader(SimulatorProcess.PARALLEL_INFO));
        for (String s : csBr.readLine().split("\\s+")) {
            csSizes.add(Integer.parseInt(s));
        }
        for (String s : parallelBr.readLine().split("\\s+")) {
            parallelisms.add(Integer.parseInt(s));
        }
        System.out.println(csSizes + " " + parallelisms);
        for (int i = 0; i < applicationName.length; i++) { //applicationName.length
            // 每个application计算一遍
//            if (i < 16) { // TODO: need to remove
//                continue;
//            }
            // 只考虑scc or 不考虑scc TODO: need to remove
//            if (applicationName[i].contains("strongly")) {
//                continue;
//            }
            String[] newApplicationName = {applicationName[i]};
            String[] newApplicationPath = {applicationPath[i]};
            for (double csRatio : cacheSpaceRatio) {
                for (double pRatio : parallelismRatio) {
                    int[] tmpCSSize = new int[replacePolicies.length];
                    Arrays.fill(tmpCSSize, NumberUtil.numberWithRatio(csSizes.get(i), csRatio));
                    int runnerSize = NumberUtil.numberWithRatio(parallelisms.get(i), pRatio); //check runner size
                    SimulatorProcess.writeExpStatistics(newApplicationName, newApplicationPath, replacePolicies, tmpCSSize,
                            runTimePath, hitRatioPath, runnerSize, true, timeBw, hitRatioBw, needHotData);
                }
            }
        }
        timeBw.close();
        hitRatioBw.close();
    }

    public static void writeExpStatistics(String[] applicationName, String[] applicationPath, ReplacePolicy[] replacePolicies,
                                          int[] cacheSpaceSize, String runTimePath, String hitRatioPath, int runnerSize, boolean appendable,
                                          BufferedWriter timeBw, BufferedWriter hitRatioBw, boolean needHotData) throws IOException {
        List<List<Double>> runTimes = new ArrayList<>();
        List<List<Double>> hitRatios = new ArrayList<>();
        for (int i = 0; i < applicationName.length; i++) {
            runTimes.add(new ArrayList<>());
            hitRatios.add(new ArrayList<>());
        }
        for (int i = 0; i < replacePolicies.length; i++) {
            List<List<Double>> expMetrics = SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath,
                    replacePolicies[i], cacheSpaceSize[i], runnerSize, needHotData);
            List<Double> curRuntime = expMetrics.get(0);
            List<Double> curHitRatio = expMetrics.get(1);
            for (int j = 0; j < curRuntime.size(); j++) {
                runTimes.get(j).add(curRuntime.get(j));
                hitRatios.get(j).add(curHitRatio.get(j));
            }
        }
//        BufferedWriter timeBw = null;
//        BufferedWriter hitRatioBw = null;
//        try {
//            timeBw = new BufferedWriter(new FileWriter(runTimePath, appendable));
//            hitRatioBw = new BufferedWriter(new FileWriter(hitRatioPath, appendable));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        for (int i = 0; i < runTimes.size(); i++) {
            String toPrintRunTime = runTimes.get(i).toString();
            timeBw.write(String.format("%s\n", toPrintRunTime.substring(1, toPrintRunTime.length() - 1)));
            String toPrintHitRatio = hitRatios.get(i).toString();
            hitRatioBw.write(toPrintHitRatio.substring(1, toPrintHitRatio.length() - 1) + "\n");
        }
//        timeBw.close();
//        hitRatioBw.close();
//        try {
//            Thread.sleep(300);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    public static List<List<Double>> processWithRuntimeCache(String[] applicationNames, String[] fileNames,
                                                             ReplacePolicy policy, int cacheSpaceSize,
                                                             int runnerSize, boolean needHotData) {
        CacheSpace cacheSpace = new CacheSpace(cacheSpaceSize, policy);
        StageDispatcher sd = new StageDispatcher("RUNTIME_CACHE", runnerSize, cacheSpace);
        List<Double> applicationTimeToPrint = new ArrayList<>();
        List<Double> hitRatio = new ArrayList<>();
        for (int i = 0; i < applicationNames.length; i++) {
            List<Double> eachJobTime = new ArrayList<>();
            String application = applicationNames[i];
            String applicationFileName = fileNames[i];
            // TODO: need to remove
//            if (!application.contains("spark_strongly")) {
//                continue;
//            }
            // TODO: need to remove
//            if (!application.contains("spark_svm")) {
//                continue;
//            }
            // KEYPOINT 原来问题出在这呢
            // get job list and hot data
            JobStageSubmitter jss = new JobStageSubmitter(application, applicationFileName);
            List<RDD> hotData = HotDataGenerator.hotRDD(application, jss.jobList, policy);
            if (!needHotData && policy != ReplacePolicy.DP) { // DP不过滤
                Map<Long, RDD> rddMap = new HashMap<>();
                for (Job job : jss.jobList) {
                    for (Stage stage : job.stages) {
                        for (RDD rdd : stage.rdds) {
                            if (!rddMap.containsKey(rdd.rddId)) {
                                rddMap.put(rdd.rddId, rdd);
                            }
                        }
                    }
                }
                hotData = new ArrayList<>(rddMap.values());
            }
            long proposedSize = HotDataGenerator.proposeCacheSpaceSize(application, hotData); // TODO: do something for the size
            // prepare for running application, StageDispatcher -> (StageRunner | CacheSpace)
            sd.prepareForNewApplication(application, jss.jobList, hotData);
            sd.initializeHotRDDOfStageRunners();
            sd.initializeCacheSpace();
            double applicationTotalTime = 0;
            for (Job job : jss.jobList) {
                curJobId = job.jobId; // for check
                double jobTotalTime = 0;
                sd.dispatchStage(jss.submitAvailableJob());
                jobTotalTime += sd.runStagesWithCacheSpace();
                List<Stage> toSubmit;
                while ((toSubmit = jss.submitAvailableStages()) != null) {
                    sd.dispatchStage(toSubmit);
                    jobTotalTime += sd.runStagesWithCacheSpace();
                }
                logger.info(String.format("SimulatorProcess: application [%s] job [%s] has run for [%f]s.)",
                        application, job.jobId, jobTotalTime));
                applicationTotalTime += jobTotalTime;
                eachJobTime.add(jobTotalTime / 1000);
            }
            logger.debug(String.format("SimulatorProcess: application [%s] has run for [%f]s.)",
                    application, applicationTotalTime));
            applicationTimeToPrint.add(applicationTotalTime);
            hitRatio.add(StageDispatcher.hitRatio());
            if (cacheSpaceSize > 99999) {
                System.out.println(String.format("Application [%s] totalTime: [%f], eachJobTime: %s.",
                        applicationNames[i], applicationTotalTime / 10, eachJobTime));
            }
        }
        for (int i = 0; i < applicationTimeToPrint.size(); i++) {
//            System.out.println(applicationTimeToPrint.get(i));
            System.out.println(hitRatio.get(i)); //%是转义符
        }
        List<List<Double>> res = new ArrayList<>();
        res.add(applicationTimeToPrint);
        res.add(hitRatio);
        return res;
    }
}
