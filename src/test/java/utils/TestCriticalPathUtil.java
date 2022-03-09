package utils;

import entity.*;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import simulator.CacheSpace;
import simulator.JobGenerator;
import simulator.ReplacePolicy;
import sketch.StaticSketch;
import task.SCacheSpace;
import task.TaskGenerator;

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

    @Test
    void testGetLongestTimeOfTaskWithPathAndCS() throws IOException {
        {
            for (int i = 0; i < applicationName.length; i++) {
                if (!applicationName[i].contains("spark_svm")) {
                    continue;
                }
                System.out.println(String.format("test application %s.", applicationName[i]));
                Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(fileName + applicationPath[i], null);
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
                Map<Long, Stage> stageMap = new HashMap<>();
                for (Job job : jobList) {
                    for (Stage stage : job.stages) {
                        stageMap.put(stage.stageId, stage);
                    }
                }
                for (List<Task> tasks : stageIdToTasks.values()) {
                    for (Task task : tasks) {
                        List<String> computePath = new ArrayList<>();
                        List<String> totalPath = new ArrayList<>();
                        List<Long> stageComputePath = new ArrayList<>();
                        stageMap.get(task.stageId).rdds.sort((o1, o2) -> (int) (o1.rddId - o2.rddId));
                        List<Long> stageTotalPath = new ArrayList<>();
                        for (RDD rdd : stageMap.get(task.stageId).rdds) {
                            stageTotalPath.add(rdd.rddId);
                        }
                        for (Partition p : task.getPartitions()) {
                            totalPath.add(p.getPartitionId());
                        }
                        totalPath.sort(String::compareTo);
                        // compute Path记录的是整理计算情况，不能直接用来作为除以时间的百分比
                        double time = CriticalPathUtil.getLongestTimeOfTaskWithSource(task, null,
                                CriticalPathUtil.TASK_LAST_NODE, computePath);
                        double compareTime = CriticalPathUtil.getLongestTimeOfTaskWithSource(task, null,
                                CriticalPathUtil.TASK_LAST_NODE, null);
//                        computePath.size();
                        Map<Long, Long> parentMap = new HashMap<>();
                        CriticalPathUtil.getLongestTimeOfTaskWithPath(task, null, parentMap);
                        long lastRDDId = SimpleUtil.lastPartitionOfTask(task).belongRDD.rddId;
                        int size = 0;
                        long start = lastRDDId + 1;
                        while (parentMap.containsKey(start)) {
                            size++;
                            start = parentMap.get(start);
                        }
                        double stageTime = CriticalPathUtil.getLongestTimeOfStageWithSource(stageMap.get(task.stageId), null,
                                CriticalPathUtil.STAGE_LAST_NODE, stageComputePath);
                        double stageCompareTime = CriticalPathUtil.getLongestTimeOfStageWithSource(stageMap.get(task.stageId), null,
                                CriticalPathUtil.STAGE_LAST_NODE, null);
                        System.out.println(String.format("Task: [%d] of Stage [%d] -> time [%f]ms, compare time [%f]ms, partition path: %s, total partitions %s.",
                                task.getTaskId(), task.stageId, time, compareTime, computePath, totalPath));
                        System.out.println(String.format("Stage: [%d] -> time [%s] ms, compare time [%f]ms, rdd path: %s, total RDDs %s.",
                                task.stageId, stageTime, stageCompareTime, stageComputePath, stageTotalPath));
                        assertEquals(compareTime, time);
                        // 正好增加了不确定性hh
                        System.out.println(String.format("[%d,%2f] -> %2f", task.getDuration(),
                                size / (double) task.getPartitions().size(), time));
//                        assertEquals(String.format("%2f", task.getDuration() * size / (double) task.getPartitions().size()),
//                                String.format("%2f", time));
                        assertEquals(stageCompareTime, stageTime);
//                        System.out.println(String.format("%f - %f : %.2f", time, stageTime,
//                                SimpleUtil.generateDifferenceRatio(stageTime, time)));
                    }
                }
            }
        }
        {
            SCacheSpace sCacheSpace = new SCacheSpace(20, ReplacePolicy.SLRU);
            {
                // add partitions
                RDD rdd = new RDD();
                rdd.rddParentIDs = new ArrayList<>();
                rdd.rddId = 38L;
                RDD rdd2 = new RDD();
                rdd2.rddParentIDs = new ArrayList<>();
                rdd2.rddId = 30L;
                for (int i = 0; i <= 9; i++) {
                    Partition p = new Partition(i, 1, rdd);
                    sCacheSpace.addPartition(p);
                    Partition p2 = new Partition(i, 1, rdd2);
                    sCacheSpace.addPartition(p2);
                }

            }
            for (int i = 0; i < applicationName.length; i++) {
//                if (!applicationName[i].contains("spark_svm")) {
//                    continue;
//                }
                System.out.println(String.format("test application %s.", applicationName[i]));
                Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(fileName + applicationPath[i], null);
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
                Map<Long, Stage> stageMap = new HashMap<>();
                for (Job job : jobList) {
                    for (Stage stage : job.stages) {
                        stageMap.put(stage.stageId, stage);
                    }
                }
                for (List<Task> tasks : stageIdToTasks.values()) {
                    for (Task task : tasks) {
                        List<String> computePath = new ArrayList<>();
                        List<String> totalPath = new ArrayList<>();
                        List<Long> stageComputePath = new ArrayList<>();
                        stageMap.get(task.stageId).rdds.sort((o1, o2) -> (int) (o1.rddId - o2.rddId));
                        List<Long> stageTotalPath = new ArrayList<>();
                        for (RDD rdd : stageMap.get(task.stageId).rdds) {
                            stageTotalPath.add(rdd.rddId);
                        }
                        for (Partition p : task.getPartitions()) {
                            totalPath.add(p.getPartitionId());
                        }
                        totalPath.sort(String::compareTo);
                        // compute Path记录的是整理计算情况，不能直接用来作为除以时间的百分比
                        double time = CriticalPathUtil.getLongestTimeOfTaskWithSource(task, sCacheSpace,
                                CriticalPathUtil.TASK_LAST_NODE, computePath);
                        double compareTime = CriticalPathUtil.getLongestTimeOfTaskWithSource(task, null,
                                CriticalPathUtil.TASK_LAST_NODE, null);
                        Map<Long, Long> parentMap = new HashMap<>();
                        CriticalPathUtil.getLongestTimeOfTaskWithPath(task, sCacheSpace, parentMap);
                        // get path length
                        long lastRDDId = SimpleUtil.lastPartitionOfTask(task).belongRDD.rddId;
                        int size = 0;
                        long start = lastRDDId + 1;
                        while (parentMap.containsKey(start)) {
                            size++;
                            start = parentMap.get(start);
                        }
                        // end get
                        // another get
                        parentMap = new HashMap<>();
                        CriticalPathUtil.getLongestTimeOfTaskWithPath(task, null, parentMap);
                        // get path length
                        lastRDDId = SimpleUtil.lastPartitionOfTask(task).belongRDD.rddId;
                        int anotherSize = 0;
                        start = lastRDDId + 1;
                        while (parentMap.containsKey(start)) {
                            anotherSize++;
                            start = parentMap.get(start);
                        }
                        // end another get
                        double stageTime = CriticalPathUtil.getLongestTimeOfStageWithSource(stageMap.get(task.stageId), null,
                                CriticalPathUtil.STAGE_LAST_NODE, stageComputePath);
                        System.out.println(String.format("Task: [%d] of Stage [%d] -> time [%f]ms, compare time [%f]ms, partition path: %s, total partitions %s.",
                                task.getTaskId(), task.stageId, time, compareTime, computePath, totalPath));
                        System.out.println(String.format("Stage: [%d] -> time [%s] ms, rdd path: %s, total RDDs %s.",
                                task.stageId, stageTime, stageComputePath, stageTotalPath));
//                        assertEquals(compareTime, time);
                        System.out.println(String.format("compute path: %s, %d / %d: %4f -> %4f", computePath, size, anotherSize, size / (double) anotherSize, time / compareTime));
                        assertTrue(String.format("%4f", size / (double) anotherSize).equals(
                                String.format("%4f", time / compareTime)) || String.format("%4f", (computePath.size() - 1) / (double) anotherSize).equals(String.format("%4f", time / compareTime)));
                        // 正好增加了不确定性hh
//                        System.out.println(String.format("[%d, %2f] -> %2f", task.getDuration(),
//                                size / (double) task.getPartitions().size(), time));
//                        assertEquals(String.format("%2f", task.getDuration() * size / (double) task.getPartitions().size()),
//                                String.format("%2f", time));
                    }
                }
            }
        }
    }

}