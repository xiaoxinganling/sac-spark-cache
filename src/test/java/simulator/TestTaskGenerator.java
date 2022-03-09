package simulator;

import entity.*;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import task.TaskGenerator;
import utils.SimpleUtil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TestTaskGenerator {


    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    String fileName = "E:\\Google Chrome Download\\";

    @Test
    void generateTaskOfApplication() throws IOException {
        {
            for (int i = 0; i < applicationName.length; i++) {
                // first to test svm
//                if(!applicationName[i].contains("spark_svm")) {
//                    continue;
//                }
                System.out.println(String.format("check task of %s", applicationName[i]));
                Map<Long, List<Task>> stageIdToTaskList = TaskGenerator.generateTaskOfApplication(fileName + applicationPath[i]);
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
                for (Job job : jobList) {
                    for (Stage stage : job.stages) {
                        List<Long> taskIds = new ArrayList<>();
                        for (Task task : stageIdToTaskList.get(stage.stageId)) {
                            taskIds.add(task.getTaskId());
                        }
                        Collections.sort(taskIds);
                        System.out.println(String.format("stage id: %d, partition example: %d, taskNum: %d, tasks: %s", stage.stageId,
                                stage.rdds.get(0).partitionNum, stage.taskNum, taskIds));
                        assertEquals(stage.taskNum, taskIds.size());
                    }
                }
//                assertTrue(new File(fileName + applicationPath[i]).exists());
//                Map<Long, List<Task>> stageIdToTaskListForContrast = TaskGenerator.generateTaskOfApplication(fileName + applicationPath[i]);
////                System.out.println(stageIdToTaskList.toString());
//                assertEquals(stageIdToTaskList.toString(), stageIdToTaskListForContrast.toString());
            }
        }
    }

    @Test
    void validateTaskAndStageTime() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
            // first to test svm
            if(!applicationName[i].contains("spark_svm")) {
                continue;
            }
            System.out.println(String.format("check task of %s", applicationName[i]));
            Map<Long, List<Task>> stageIdToTaskList = TaskGenerator.generateTaskOfApplication(fileName + applicationPath[i]);
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            List<Double> stageTime = new ArrayList<>(), taskTime = new ArrayList<>();
            for (Job job : jobList) {
                for (Stage stage : job.stages) {
                    System.out.println("===================================");
                    List<Task> taskList = stageIdToTaskList.get(stage.stageId);
                    taskList.sort((o1, o2) -> (int) (o1.getTaskId() - o2.getTaskId()));
                    long minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
                    for (Task task : taskList) {
                        minTime = Math.min(minTime, task.taskInfo.startTime);
                        maxTime = Math.max(maxTime, task.taskInfo.finishTime);
                    }
                    for (Task task : taskList) {
                        task.setDuration(maxTime - minTime);
                        double curStageTime = (stage.completeTime - stage.submitTime) / 1000.0, curTaskTime = task.getDuration() / 1000.0;
//                        double curStageTime = stage.completeTime - stage.submitTime, curTaskTime = task.getDuration();
                        System.out.println(String.format("Stage id [%d], time: [%f], Task id [%d], time: [%f].", stage.stageId, curStageTime,
                                task.getTaskId(), curTaskTime));
                        stageTime.add(curStageTime);
                        taskTime.add(curTaskTime);
                    }
//                    System.out.println(String.format("stage: [%d-%d: %d], task: [%d-%d: %d]", stage.submitTime,
//                            stage.completeTime, stage.completeTime - stage.submitTime, minTime, maxTime, maxTime - minTime));
//                    System.out.println(String.format("%.2f%%", Math.abs(maxTime - minTime - (stage.completeTime - stage.submitTime)) / (double) (
//                            stage.completeTime - stage.submitTime) * 100));
                }
            }
            for (double sTime : stageTime) {
                System.out.println(sTime);
            }
            System.out.println("======= " + stageTime.size());
            for (double tTime : taskTime) {
                System.out.println(tTime);
            }
//            System.out.println("=======");
//            for (int j = 1; j <= stageTime.size(); j++) {
//                System.out.println(j);
//            }
        }
    }

    @Test
    void testGenerateTaskMap() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
            // first to test svm
            if(!applicationName[i].contains("spark_svm")) {
                continue;
            }
            System.out.println(String.format("check task of %s", applicationName[i]));
            Map<Long, List<Task>> stageIdToTaskList = TaskGenerator.generateTaskOfApplication(fileName + applicationPath[i]);
            List<Long> taskIds = new ArrayList<>(TaskGenerator.generateTaskMap(stageIdToTaskList).keySet());
            System.out.println(taskIds);
            assertEquals(taskIds.size(), 140);
        }
    }

    @Test
    void testUpdateTaskPartitionAndIndex() throws IOException { // 兼测试task与partition情况
        {
            for (int i = 0; i < applicationName.length; i++) {
                // first to test svm, comment to all spark application
                if(!applicationName[i].contains("spark_svm")) {
                    continue;
                }
//                if(!applicationName[i].contains("spark_strongly")) {
//                    continue;
//                }
                System.out.println(String.format("check task of %s", applicationName[i]));
                Map<Long, List<Task>> stageIdToTaskList = TaskGenerator.generateTaskOfApplicationV2(fileName + applicationPath[i], null);
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
                Map<Long, Stage> stageMap = new HashMap<>();
                double totalStageSize = 0, unhealthyStageSize = 0;
                for (Job job : jobList) {
                    for (Stage stage : job.stages) {
                        totalStageSize++;
                        long max = 0;
                        long total = 0;
                        boolean relatedToHDFS = false;
                        for (RDD rdd : stage.rdds) {
                            total += rdd.partitionNum;
                            if (rdd.partitionNum > max) {
                                max = rdd.partitionNum;
                            }
                        }
                        for (RDD rdd : stage.rdds) {
                            if (relatedToHDFS) {
                                break;
                            }
                            if (rdd.partitionNum == max) {
                                relatedToHDFS = rdd.rddName.contains("hdfs://");
                            }
                        }
                        int taskSize = stageIdToTaskList.get(stage.stageId).size();
                        if (max != stageIdToTaskList.get(stage.stageId).size()) {
                            unhealthyStageSize++;
                            System.out.println(String.format("App [%s] Job [%s] Stage [%s]'s max partitionNum [%d] != task size [%d], dr: [%2f%%].",
                                    applicationName[i], job.jobId, stage.stageId, max, taskSize, SimpleUtil.generateDifferenceRatio(max, taskSize) * 100));
//                            assertTrue(relatedToHDFS);
                            if (!relatedToHDFS) {
                                System.err.println(String.format("App [%s] Job [%s] Stage [%s]'s max partitionNum [%d] != task size [%d], dr: [%2f%%].",
                                        applicationName[i], job.jobId, stage.stageId, max, taskSize, SimpleUtil.generateDifferenceRatio(max, taskSize) * 100));
                            }
                            if (taskSize == 1) {
                                Task task = stageIdToTaskList.get(stage.stageId).get(0);
                                System.err.println(String.format("App [%s] Job [%s] Stage [%s]'s max partitionNum [%d] == task partition size [%d], dr: [%2f%%].",
                                        applicationName[i], job.jobId, stage.stageId, total, task.getPartitions().size(), SimpleUtil.generateDifferenceRatio(total, task.getPartitions().size()) * 100));
                                assertEquals(total, task.getPartitions().size());
                            }

                        }
                        stageMap.put(stage.stageId, stage);
                    }
                }
                System.out.println(String.format("%s unhealthy ratio [%f / %f]: %2f%%",
                        applicationName[i], unhealthyStageSize, totalStageSize,
                        unhealthyStageSize / totalStageSize * 100));
                for (Map.Entry<Long, List<Task>> entry : stageIdToTaskList.entrySet()) {
                    long stageId = entry.getKey();
                    Stage curStage = stageMap.get(stageId);
                    Set<String> relationInStage = new HashSet<>();
                    Set<String> relationInTask = new HashSet<>();
                    Set<Long> rddIdSet = new HashSet<>();
                    for (RDD rdd : curStage.rdds) {
                        rddIdSet.add(rdd.rddId);
                    }
                    for (RDD rdd : curStage.rdds) {
                        for (long parentId : rdd.rddParentIDs) {
                            if (rddIdSet.contains(parentId)) {
                                relationInStage.add(String.format("%d_%d", parentId, rdd.rddId));
                            }
                        }
                    }
                    List<Task> taskList = entry.getValue();
//                    System.out.println(String.format("Stage id : %d", entry.getKey()));
                    for (int j = 0; j < taskList.size(); j++) {
                        Task curTask = taskList.get(j);
                        assertEquals(j, curTask.getIndexInStage());
                        int rddPartitionSize = 0;
                        for (RDD rdd : curStage.rdds) {
                            rddPartitionSize += rdd.partitionNum;
                        }
                        assertTrue(curStage.rdds.size() == curTask.getPartitions().size() ||
                                rddPartitionSize == curTask.getPartitions().size());
                        for (Partition p : curTask.getPartitions()) {
                            for (String parentId : p.getParentIds()) {
                                String parentIndex = parentId.split("_")[0];
                                if (!rddIdSet.contains(Long.parseLong(parentIndex))) {
                                    continue;
                                }
                                String selfIndex = p.getPartitionId().split("_")[0];
                                relationInTask.add(String.format("%s_%s", parentIndex, selfIndex));
                            }
                        }
                    }
//                    assertEquals(relationInStage.size(), relationInTask.size());
                    for (String s : relationInStage) {
                        assertTrue(relationInTask.contains(s));
                    }
                }
            }
        }
    }

    @Test
    void testPartitionMemorySize() throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter("a_a_partition_size_central"));
        {
            for (int i = 0; i < applicationName.length; i++) {
                // first to test svm, comment to all spark application
//                if(!applicationName[i].contains("spark_svm")) {
//                    continue;
//                }
//                if(!applicationName[i].contains("spark_strongly")) {
//                    continue;
//                }
                System.out.println(String.format("check task of %s", applicationName[i]));
                Map<Long, List<Task>> stageIdToTaskList = TaskGenerator.generateTaskOfApplication(fileName + applicationPath[i]);
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
                Map<String, List<Integer>> partitionSizeMap = TaskGenerator.updateTaskPartitionAndIndexAndCPU(stageIdToTaskList, jobList);
                TaskGenerator.updatePartitionSizes(partitionSizeMap, stageIdToTaskList);
                List<String> keyList = new ArrayList<>(partitionSizeMap.keySet());
                Map<String, Integer> partitionIdToMemorySize = new HashMap<>();
                keyList.sort(String::compareTo);
                for (String key : keyList) {
                    List<Integer> sizeList = partitionSizeMap.get(key);
                    System.out.println(String.format("partition: [%s], total size: [%d], detail: %s", key, sizeList.size(), sizeList));
                    int min = Integer.MAX_VALUE;
                    for (int size : sizeList) {
                        min = Math.min(min, size);
                        // 应该write min
                    }
                    bw.write(String.format("%d\n", min));
                    partitionIdToMemorySize.put(key, min);
                }
                for (Map.Entry<Long, List<Task>> entry : stageIdToTaskList.entrySet()) {
                    long stageId = entry.getKey();
                    List<Task> taskList = entry.getValue();
                    // do something
                    for (Task task : taskList) {
                        for (Partition p : task.getPartitions()) {
                            assertEquals(p.getMemorySize(), partitionIdToMemorySize.get(p.getPartitionId()));
                        }
                    }
                }
            }
        }
        bw.close();
    }

}