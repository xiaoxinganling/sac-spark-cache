package simulator;

import entity.Job;
import entity.Stage;
import entity.Task;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;

import java.io.File;
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
                if(!applicationName[i].contains("spark_svm")) {
                    continue;
                }
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

}