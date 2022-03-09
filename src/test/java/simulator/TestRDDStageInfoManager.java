package simulator;

import entity.*;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import task.TaskGenerator;
import utils.CriticalPathUtil;
import utils.SimpleUtil;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static simulator.RDDStageInfoManager.MAX_DISTANCE;
import static simulator.RDDStageInfoManager.MAX_TASK_DISTANCE;

class TestRDDStageInfoManager {

    // KEYPOINT: 这也是个很好的辅助功能： rddIdToStageIds, stageIdToRDDIds
    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    String fileName = "E:\\Google Chrome Download\\";
//    String[] fileNames = {StaticSketch.applicationPath[5]};
//    String[] applicationNames = {applicationName[5]}; // svm 5, svm, 永远滴工具人！
    String[] fileNames = applicationPath;
    String[] applicationNames = applicationName;

    @Test
    void testGenerateDistanceForHotData() throws IOException {
        for (int i = 0; i < fileNames.length; i++) {
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
            List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList, null);
            Map<Long, PriorityQueue<Long>> rddToStageIds = RDDStageInfoManager.generateDistanceForHotData(jobList, hotData);
            System.out.println(rddToStageIds);
            System.out.println(RDDStageInfoManager.generateStageToRDDIds(rddToStageIds));
        }
    }

    @Test
    void testUpdateDistance() throws IOException {
        for (int i = 0; i < fileNames.length; i++) {
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
            List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList, null);
            Map<Long, PriorityQueue<Long>> rddToStageIds = RDDStageInfoManager.generateDistanceForHotData(jobList, hotData);
            Set<Long> hotDataIds = new HashSet<>();
            for (RDD rdd : hotData) {
                hotDataIds.add(rdd.rddId);
            }
            System.out.println(rddToStageIds);
            for (Job job : jobList) {
                // 肯定是执行顺序的一种
                job.stages.sort((o1, o2) -> (int) (o1.stageId - o2.stageId)); // FIXME: 目前还是按照从小到大的顺序来
                for (Stage stage : job.stages) {
                    // 肯定会报错 => ? 竟然没报错
                    System.out.println("asserting stage " + stage.stageId + " -> " + SimpleUtil.cachedRDDIdSetInStage(stage, hotDataIds));
                    RDDStageInfoManager.updateDistance(rddToStageIds, stage);
                    System.out.println(rddToStageIds);
                }
            }
            for (PriorityQueue<Long> value : rddToStageIds.values()) {
                assertEquals(1, value.size());
                assertEquals(MAX_DISTANCE, value.peek());
            }
        }
    }


    @Test
    void testUpdateTaskDistance() throws IOException {
        for (int i = 0; i < fileNames.length; i++) {
//            if (!applicationNames[i].contains("spark_svm")) {
//                continue;
//            }
            System.out.println("asserting application " + applicationNames[i]);
            Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(fileName + fileNames[i],
                    applicationNames[i]);
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
            List<RDD> hotRDDs = HotDataGenerator.hotRDD(applicationName[i], jobList, null);
            // get hot partitions
            Set<Long> hotRDDIdSet = new HashSet<>();
            for (RDD rdd : hotRDDs) {
                hotRDDIdSet.add(rdd.rddId);
            }
            Map<String, Partition> hotPartitionMap = HotDataGenerator.generateHotPartitionMap(hotRDDIdSet, stageIdToTasks);
            List<Partition> hotPartitions = new ArrayList<>(hotPartitionMap.values());
            Map<String, PriorityQueue<Long>> partitionToTaskIds = RDDStageInfoManager.generateDistanceForHotPartitions(
                    stageIdToTasks, hotPartitions);
            for (Map.Entry<String, PriorityQueue<Long>> entry : partitionToTaskIds.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
            for (List<Task> tasks : stageIdToTasks.values()) {
                for (Task t : tasks) {
                    for (Partition p : t.getPartitions()) {
                        long rddId = Long.parseLong(p.getPartitionId().split(CriticalPathUtil.PARTITION_FLAG)[0]);
                        if (hotRDDIdSet.contains(rddId)) {
                            PriorityQueue<Long> taskIds = partitionToTaskIds.get(p.getPartitionId());
                            assertTrue(taskIds.contains(t.getTaskId()));
                        }
                    }
                }
            }
            for (List<Task> tasks : stageIdToTasks.values()) {
                // 要求task执行顺序按照id来
                for (Task t : tasks) {
                    RDDStageInfoManager.updatePartitionDistance(partitionToTaskIds, t);
                }
            }
            for (PriorityQueue<Long> value : partitionToTaskIds.values()) {
                assertEquals(1, value.size());
                assertEquals(MAX_TASK_DISTANCE, value.peek());
            }
        }
    }

}