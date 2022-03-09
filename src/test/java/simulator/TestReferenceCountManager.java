package simulator;

import entity.*;
import entity.event.JobStartEvent;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import task.TaskGenerator;
import utils.CacheSketcher;
import utils.CriticalPathUtil;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TestReferenceCountManager {


    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    String fileName = "E:\\Google Chrome Download\\";
//    String[] fileNames = {StaticSketch.applicationPath[5]};
//    String[] applicationNames = {applicationName[5]}; // svm 5
    String[] fileNames = applicationPath;
    String[] applicationNames = applicationName;

    @Test
    void generateRefCountForHotData() throws IOException {
        {
            for (int i = 0; i < fileNames.length; i++) {
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
                List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList, null);
                Set<Long> hotDataIds = new HashSet<>();
                List<JobStartEvent> jseList = new ArrayList<>(jobList);
                Map<Long, Integer> hotDataRC = ReferenceCountManager.generateRefCountForHotData(jobList, hotData);
                Map<Long, Integer> totalRC = CacheSketcher.generateRDDDirectRefForJobs(jseList, null);
                for (RDD rdd : hotData) {
                    hotDataIds.add(rdd.rddId);
                }
                System.out.println(hotDataIds + " -> " + hotDataRC);
                System.out.println(totalRC);//其实可打印可不打印
                assertEquals(hotDataIds.size(), hotDataRC.size());
                for(Map.Entry<Long, Integer> entry : hotDataRC.entrySet()) {
                    assertEquals(totalRC.get(entry.getKey()), entry.getValue());
                }
            }
        }
    }

    @Test
    void updateHotDataRefCount() throws IOException { // 吐槽一下，我这把别人的工作也实现得也太好了= =
        {
            // 测试最终hotDataRC所有value为0
            for (int i = 0; i < fileNames.length; i++) {
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
                List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList, null);
                Map<Long, Integer> rddIdToActionNum = ReferenceCountManager.generateRDDIdToActionNum(jobList);
                Map<Long, Integer> hotDataRC = ReferenceCountManager.generateRefCountForHotData(jobList, hotData);
                System.out.println(hotDataRC);
                for (Job job : jobList) {
                    System.out.println("job " + job.jobId +  " before: " + hotDataRC);
                    for (Stage stage: job.stages) {
                        for (RDD rdd : stage.rdds) {
                            ReferenceCountManager.updateHotDataRefCountByRDD(hotDataRC, rdd, rddIdToActionNum);
                        }
                    }
                    System.out.println("job " + job.jobId +  " after:  " + hotDataRC);
                }
                for (int value : hotDataRC.values()) {
                    assertEquals(0, value);
                }
            }
        }
    }

    // 想要自己每行代码都没有bug这件事是一种奢望
    @Test
    void testUpdateHotPartitionRC() throws IOException {
        {
            // 测试最终hotDataRC所有value为0
            for (int i = 0; i < fileNames.length; i++) {
//                if (!applicationNames[i].contains("spark_svm")) {
//                    continue;
//                }
                Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(
                        fileName + fileNames[i], applicationNames[i]);
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
                List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList, null);
                Set<Long> hotRDDIdSet = new HashSet<>();
                for (RDD rdd : hotData) {
                    hotRDDIdSet.add(rdd.rddId);
                }
                Map<String, Partition> hotPartitionMap = HotDataGenerator.generateHotPartitionMap(hotRDDIdSet, stageIdToTasks);
                List<Partition> hotPartitions = new ArrayList<>(hotPartitionMap.values());
                // 必须先hPRC、而后pITAN
                Map<String, Integer> hotPartitionRC = ReferenceCountManager.generateRefCountForHotPartition(jobList, hotPartitions, stageIdToTasks);
                Map<String, Integer> partitionIdToActionNum = ReferenceCountManager.generatePartitionIdToActionNum(jobList);
                // for compare
                Map<Long, Integer> hotDataRC = ReferenceCountManager.generateRefCountForHotData(jobList, hotData);
                Map<Long, Integer> rddIdToActionNum = ReferenceCountManager.generateRDDIdToActionNum(jobList);
                // end for compare
                Map<Long, Integer> fakeRDDCountMap = new HashMap<>();
                for (Map.Entry<String, Integer> entry : hotPartitionRC.entrySet()) {
                    System.out.println(entry);
                    long fakeRDDId = Long.parseLong(entry.getKey().split(CriticalPathUtil.PARTITION_FLAG)[0]);
//                    assertTrue(!fakeRDDCountMap.containsKey(fakeRDDId) || fakeRDDCountMap.get(fakeRDDId).equals(entry.getValue()));
                    if (!(!fakeRDDCountMap.containsKey(fakeRDDId) || fakeRDDCountMap.get(fakeRDDId).equals(entry.getValue()))) {
                        System.out.println(fakeRDDId + "=======================");
                    }
                    fakeRDDCountMap.put(fakeRDDId, entry.getValue());
                }
                System.out.println(hotPartitionRC);
                System.out.println(fakeRDDCountMap);
                System.out.println(hotDataRC);
                System.out.println(partitionIdToActionNum);
                System.out.println(rddIdToActionNum);
                for (List<Task> tasks : stageIdToTasks.values()) {
                    for (Task t : tasks) {
                        ReferenceCountManager.updateHotPartitionRefCountByTask(hotPartitionRC, t, partitionIdToActionNum);
                    }
                }
                for (int value : hotPartitionRC.values()) {
                    assertEquals(0, value);
                }
                for (int value : partitionIdToActionNum.values()) {
                    assertEquals(0, value);
                }
//                for (Job job : jobList) {
//                    System.out.println("job " + job.jobId +  " before: " + hotDataRC);
//                    for (Stage stage : job.stages) {
//                        ReferenceCountManager.updateHotDataRefCountByStage(hotDataRC, stage, rddIdToActionNum);
//                    }
//                    System.out.println("job " + job.jobId +  " after:  " + hotDataRC);
//                }
//                for (int value : hotDataRC.values()) {
//                    assertEquals(0, value);
//                }
            }
        }
//        {
//            for (int i = 0; i < fileNames.length; i++) {
//                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
//                List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList, null);
//                Map<Long, Integer> rddIdToActionNum = ReferenceCountManager.generateRDDIdToActionNum(jobList);
//                Map<Long, Integer> hotDataRC = ReferenceCountManager.generateRefCountForHotData(jobList, hotData);
//                System.out.println(hotDataRC);
//                for (Job job : jobList) {
//                    System.out.println("job " + job.jobId +  " before: " + hotDataRC);
//                    for (Stage stage: job.stages) {
//                        for (RDD rdd : stage.rdds) {
//                            ReferenceCountManager.updateHotDataRefCountByRDD(hotDataRC, rdd, rddIdToActionNum);
//                        }
//                    }
//                    System.out.println("job " + job.jobId +  " after:  " + hotDataRC);
//                }
//                for (int value : hotDataRC.values()) {
//                    assertEquals(0, value);
//                }
//            }
//        }
    }

    @Test
    void updateHotDataRefCountByStage() throws IOException { // 吐槽一下，我这把别人的工作也实现得也太好了= =
        {
            // 测试最终hotDataRC所有value为0
            for (int i = 0; i < fileNames.length; i++) {
//                Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(
//                        fileName + fileNames[i], applicationNames[i]);
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
                List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList, null);
                Map<Long, Integer> hotDataRC = ReferenceCountManager.generateRefCountForHotData(jobList, hotData);
                Map<Long, Integer> rddIdToActionNum = ReferenceCountManager.generateRDDIdToActionNum(jobList);
//                Set<Long> hotRDDIdSet = new HashSet<>();
//                for (RDD rdd : hotData) {
//                    hotRDDIdSet.add(rdd.rddId);
//                }
//                Map<String, Partition> hotPartitionMap = HotDataGenerator.generateHotPartitionMap(hotRDDIdSet, stageIdToTasks);
//                List<Partition> hotPartitions = new ArrayList<>(hotPartitionMap.values());
//                Map<String, Integer> partitionIdToActionNum = ReferenceCountManager.generatePartitionIdToActionNum(jobList);
//                Map<String, Integer> hotPartitionRC = ReferenceCountManager.generateRefCountForHotPartition(jobList, hotPartitions, stageIdToTasks);
//                System.out.println(hotPartitionRC);
//                System.out.println(partitionIdToActionNum);
//                for (List<Task> tasks : stageIdToTasks.values()) {
//                    for (Task t : tasks) {
//                        ReferenceCountManager.updateHotPartitionRefCountByTask(hotPartitionRC, t, partitionIdToActionNum);
//                    }
//                }
//                for (int value : hotPartitionRC.values()) {
//                    assertEquals(0, value);
//                }
//                for (int value : partitionIdToActionNum.values()) {
//                    assertEquals(0, value);
//                }
                for (Job job : jobList) {
                    System.out.println("job " + job.jobId +  " before: " + hotDataRC);
                    for (Stage stage : job.stages) {
                        ReferenceCountManager.updateHotDataRefCountByStage(hotDataRC, stage, rddIdToActionNum);
                    }
                    System.out.println("job " + job.jobId +  " after:  " + hotDataRC);
                }
                for (int value : hotDataRC.values()) {
                    assertEquals(0, value);
                }
            }
        }
    }

}