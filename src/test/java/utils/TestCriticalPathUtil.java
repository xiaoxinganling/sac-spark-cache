package utils;

import entity.Job;
import entity.RDD;
import entity.Stage;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import simulator.CacheSpace;
import simulator.JobGenerator;
import simulator.ReplacePolicy;
import sketch.StaticSketch;
import java.io.IOException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class TestCriticalPathUtil {

    String fileName = "E:\\Google Chrome Download\\";
    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    Logger logger = Logger.getLogger(TestCriticalPathUtil.class);

    @Test
    void testGetLongestTimeOfStage() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
            if(!applicationName[i].contains("spark_svm")) {
                continue;
            }
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
            logger.info("test stage time of " + applicationName[i]);
            int curStage = 0;
            for(StageCompletedEvent sce : stageList) {
                Map<Long, RDD> rddMap = new HashMap<>();
                for(RDD rdd : sce.stage.rdds) {
                    rddMap.put(rdd.rddId, rdd);
                }
                curStage++;
                RDD lastRDD = SimpleUtil.lastRDDOfStage(sce.stage);
                logger.info("rdd size in Stage——" + sce.stage.stageId + ": " + rddMap.size());
//                logger.info(curStage + "/" + stageList.size() + ": " + sce.stage.stageId + "——" + SimpleUtil.lastRDDTimeOfStage(rddMap, lastRDD));
                logger.info(curStage + "/" + stageList.size() + ": " + sce.stage.stageId + "——" + CriticalPathUtil.getLongestTimeOfStageWithSource(sce.stage, null, CriticalPathUtil.STAGE_LAST_NODE, CriticalPathUtil.NO_NEED_FOR_PATH));
            }
        }
    }

    @Test
    void testGetLongestTimeOfStageWithPath() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
            if(!applicationName[i].contains("spark_svm")) {
                continue;
            }
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
            logger.info("test stage time of " + applicationName[i]);
            int curStage = 0;
            for(StageCompletedEvent sce : stageList) {
                Map<Long, RDD> rddMap = new HashMap<>();
                for(RDD rdd : sce.stage.rdds) {
                    rddMap.put(rdd.rddId, rdd);
                }
                curStage++;
                logger.info("rdd size in Stage——" + sce.stage.stageId + ": " + rddMap.size());
//                logger.info(curStage + "/" + stageList.size() + ": " + sce.stage.stageId + "——" + SimpleUtil.lastRDDTimeOfStage(rddMap, lastRDD));
//                logger.info(curStage + "/" + stageList.size() + ": " + sce.stage.stageId + "——" + CriticalPathUtil.getLongestTimeOfStage(sce.stage, null));
                Map<Long, Long> pathMap = new HashMap<>();
                System.out.println(CriticalPathUtil.getLongestTimeOfStageWithPath(sce.stage, null, pathMap)); // 先取lastRDDId + 1，然后以此类推
                System.out.println(pathMap);
            }
        }
    }

    @Test
    void testGetKeyStagesOfJob() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
            if (!applicationName[i].contains("spark_svm")) {
                continue;
            }
            // 往后所有只需要job List的地方都用这个函数
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            for (Job job : jobList) {
                System.out.println("Job : " + job.jobId + " -> " + CriticalPathUtil.getKeyStagesOfJob(job).keySet());
            }
        }
    }

    @Test
    void testGetKeyStagesOfJobList() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
//            if (!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
            // 往后所有只需要job List的地方都用这个函数
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            Set<Long> individualCall = new HashSet<>();
            for (Job job : jobList) {
                individualCall.addAll(CriticalPathUtil.getKeyStagesOfJob(job).keySet());
            }
            Set<Long> totalSet = CriticalPathUtil.getKeyStagesOfJobList(jobList).keySet();
            System.out.println("Application : " + applicationName[i] + "-> \n" + totalSet);
            System.out.println(individualCall);
            assertEquals(individualCall.toString(), totalSet.toString()); //因为两者的内容一样，而且都无序，所以toString()也一样
        }
    }

    @Test
    void testGetLongestTimeOfStageWithDifferentSource() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
            if (!applicationName[i].contains("spark_svm")) {
                continue;
            }
            // 往后所有只需要job List的地方都用这个函数
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            for (Job job : jobList) {
                for (Stage stage : job.stages) {
                    List<RDD> rddList = new ArrayList<>(stage.rdds);
                    for (RDD rdd : rddList) {
                        // 迭代时不能修改，不然会报错ConcurrentModificationException
                        double time = CriticalPathUtil.getLongestTimeOfStageWithSource(stage, null, rdd.rddId, CriticalPathUtil.NO_NEED_FOR_PATH);
                        System.out.println(String.format("Stage: [%d] -> save [%d] for [%f]s.",
                                stage.stageId, rdd.rddId, time));
                    }
                }
            }
        }
    }

    @Test
    void testGetLongestTimeOfStageWithPathAndCS() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
//            if (!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
            // 往后所有只需要job List的地方都用这个函数
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            CacheSpace cs = new CacheSpace(20, ReplacePolicy.DP);
            RDD tmp1 = new RDD();
            tmp1.rddId = 3L;
            tmp1.partitionNum = 10L;
            RDD tmp2 = new RDD();
            tmp2.rddId = 10L;
            tmp2.partitionNum = 10L;
            cs.addRDD(tmp1);
            cs.addRDD(tmp2);
            for (Job job : jobList) {
                for (Stage stage : job.stages) {
                    List<Long> totalRDD = new ArrayList<>();
                    for (RDD rdd :stage.rdds) {
                        totalRDD.add(rdd.rddId);
                    }
                    totalRDD.sort((o1, o2) -> (int) (o1 - o2));
                    List<Long> rddIdPath = new ArrayList<>();
                    double time = CriticalPathUtil.getLongestTimeOfStageWithSource(stage, cs,
                            CriticalPathUtil.STAGE_LAST_NODE, rddIdPath);
                    System.out.println(String.format("Stage: [%d] -> rdd path: %s, total RDDs %s, time [%f]s.",
                            stage.stageId, rddIdPath, totalRDD, time));
                }
            }
        }
    }

}