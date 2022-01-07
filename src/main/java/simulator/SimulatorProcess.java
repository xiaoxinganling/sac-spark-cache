package simulator;

import entity.Job;
import entity.RDD;
import entity.Stage;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class SimulatorProcess {

    private static Logger logger = Logger.getLogger(SimulatorProcess.class);

    public static long curJobId = -1;

    public static void processWithNoCache(String[] applicationNames, String[] fileNames) {
        StageDispatcher sd = new StageDispatcher("NO_CACHE", 4);
        List<Double> applicationTimeToPrint = new ArrayList<>();
        for(int i = 0; i < applicationNames.length; i++) {
            String application = applicationNames[i];
            String applicationFileName = fileNames[i];
            JobStageSubmitter jss = new JobStageSubmitter(application, applicationFileName);
            double applicationTotalTime = 0;
            for(Job job : jss.jobList) {
                double jobTotalTime = 0;
                sd.dispatchStage(jss.submitAvailableJob());
                jobTotalTime += sd.runStages();
                List<Stage> toSubmit;
                while((toSubmit = jss.submitAvailableStages()) != null) {
                    sd.dispatchStage(toSubmit);
                    jobTotalTime += sd.runStages();
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

    public static void processWithRuntimeCache(String[] applicationNames, String[] fileNames, ReplacePolicy policy, int cacheSpaceSize) {
        CacheSpace cacheSpace = new CacheSpace(cacheSpaceSize, policy);
        StageDispatcher sd = new StageDispatcher("RUNTIME_CACHE", 4, cacheSpace);
        List<Double> applicationTimeToPrint = new ArrayList<>();
        for(int i = 0; i < applicationNames.length; i++) {
            String application = applicationNames[i];
            String applicationFileName = fileNames[i];
//            if (!application.contains("spark_strongly")) {
//                continue;
//            }
            // get job list and hot data
            JobStageSubmitter jss = new JobStageSubmitter(application, applicationFileName);
            List<RDD> hotData = HotDataGenerator.hotRDD(application, jss.jobList);
            long proposedSize =  HotDataGenerator.proposeCacheSpaceSize(application, hotData); // TODO: do something for the size
            // prepare for running application, StageDispatcher -> (StageRunner | CacheSpace)
            sd.prepareForNewApplication(application, jss.jobList, hotData);
            sd.initializeHotRDDOfStageRunners();
            sd.initializeCacheSpace();
            double applicationTotalTime = 0;
            for(Job job : jss.jobList) {
                curJobId = job.jobId; // for check
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

}
