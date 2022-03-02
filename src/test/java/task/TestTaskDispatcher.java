package task;

import entity.TSDecision;
import entity.Task;
import org.junit.jupiter.api.Test;
import simulator.TaskGenerator;
import sketch.StaticSketch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class TestTaskDispatcher {

    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    String fileName = "E:\\Google Chrome Download\\";

    @Test
    @Deprecated
    void testDispatchAndRunTask() throws IOException {
        {
            for (int i = 0; i < applicationName.length; i++) {
//                 first to test svm
                if(!applicationName[i].contains("spark_svm")) {
                    continue;
                }
                List<Double> tmpTime = new ArrayList<>();
                Map<Long, List<Task>> stageIdToTaskList = TaskGenerator.generateTaskOfApplication(fileName + applicationPath[i]);
                TaskGenerator.updateTaskTimeWithMaxTime(stageIdToTaskList); // TODO: need to determine to remove
                // TODO: 一波一波调度: => 验证正确性 (SR设置成20)
//                for (int j = 1; j <= 200; j++) {
//                    TaskDispatcher taskDispatcher = new TaskDispatcher(String.format("TD-%s", applicationName[i]), j);
//                    taskDispatcher.dispatchTask(stageIdToTaskList);
//                    double curTime = taskDispatcher.runTasks() / 1000.0;
//                    System.out.println(String.format("Application [%s] run for [%f] s.", applicationName[i], curTime));
//                    tmpTime.add(curTime);
//                }
//                for (int j = 0; j < tmpTime.size(); j++) {
//                    System.out.println(j + 1);
//                }
//                System.out.println("=============");
//                for (double d : tmpTime) {
//                    System.out.println(d);
//                }
                TaskDispatcher taskDispatcher = new TaskDispatcher(String.format("TD-%s", applicationName[i]), 40);
                List<TSDecision> tsDecisions = taskDispatcher.dispatchTask(stageIdToTaskList, null);
                double curTime = taskDispatcher.runTasks(tsDecisions, null) / 1000.0;
                System.out.println(String.format("Application [%s] run for [%f] s.", applicationName[i], curTime));
                tmpTime.add(curTime);
            }
        }
    }

    @Test
    void testDispatchAndRunTaskV2() throws IOException {
        {
            for (int i = 0; i < applicationName.length; i++) {
                // first to test svm
                if(!applicationName[i].contains("spark_pregel")) { //spark_strongly
                    continue;
                }
                List<Double> tmpTime = new ArrayList<>();
                Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplication(fileName + applicationPath[i]);
                TaskGenerator.updateTaskTimeWithMaxTime(stageIdToTasks);
                for (int j = 1; j <= 200; j++) {
                    TaskDispatcher taskDispatcher = new TaskDispatcher(String.format("TD-%s", applicationName[i]), j);
                    double curTime = taskDispatcher.dispatchAndRunTask(applicationName[i],
                            fileName + applicationPath[i], stageIdToTasks);
                    System.out.println(String.format("Application [%s] run for [%f] s with [%d] TaskRunners.",
                            applicationName[i], curTime / 1000, j));
                    tmpTime.add(curTime / 1000);
                }
                for (double d : tmpTime) {
                    System.out.println(d);
                }
            }
        }
    }

    @Test
    void testDispatchAndRunTaskWithNTR() throws IOException {
        {
            int taskRunnerSize = 100;
            // [SVM 137696.75]
            List<Double> totalTime = new ArrayList<>();
            for (int i = 0; i < applicationName.length; i++) {
                // first to test svm
//                if(!applicationName[i].contains("spark_svm")) {
//                    continue;
//                }
                Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplication(fileName + applicationPath[i]);
                TaskGenerator.updateTaskTimeWithMaxTime(stageIdToTasks);
                TaskDispatcher taskDispatcher = new TaskDispatcher(String.format("TD-%s", applicationName[i]), taskRunnerSize);
                double curTime = taskDispatcher.dispatchAndRunTask(applicationName[i],
                        fileName + applicationPath[i], stageIdToTasks);
                System.out.println(String.format("Application [%s] run for [%f] s with [%d] TaskRunners.",
                        applicationName[i], curTime / 1000, taskRunnerSize));
                totalTime.add(curTime / 1000);
            }
            System.out.println("=============================");
            for (double time : totalTime) {
                System.out.println(time);
            }
        }
    }

}