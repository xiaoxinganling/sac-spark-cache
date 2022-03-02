package task.scheduler;

import entity.Job;
import entity.Stage;
import entity.Task;
import org.junit.jupiter.api.Test;
import simulator.JobGenerator;
import simulator.TaskGenerator;
import sketch.StaticSketch;
import task.TaskDispatcher;
import utils.SimpleUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TestFIFOScheduler {

    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    String fileName = "E:\\Google Chrome Download\\";

    @Test
    void testScheduleAndRunTasks() throws IOException {
        {
            int taskRunnerSize = 100;
            TaskDispatcher taskDispatcher = new TaskDispatcher("TD-Default", taskRunnerSize);
            FIFOScheduler fifoScheduler = new FIFOScheduler(taskDispatcher);
            // [SVM 137696.75, 143.073]
            List<Double> totalTime = new ArrayList<>();
            for (int i = 0; i < applicationName.length; i++) {
                // first to test svm
//                if(!applicationName[i].contains("spark_svm")) {
//                    continue;
//                }
                Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplication(fileName + applicationPath[i]);
                TaskGenerator.updateTaskTimeWithMaxTime(stageIdToTasks);
                Map<Long, Task> taskMap = TaskGenerator.generateTaskMap(stageIdToTasks);
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
                double time = 0;
                for (Job job : jobList) {
                    time += fifoScheduler.runTask(fifoScheduler.schedule(job.stages, stageIdToTasks),
                            false, taskMap);
                }
//                double curTime = taskDispatcher.dispatchAndRunTask(applicationName[i],
//                        fileName + applicationPath[i], stageIdToTasks);
                System.out.println(String.format("Application [%s] run for [%f] s with [%d] TaskRunners.",
                        applicationName[i], time / 1000, taskRunnerSize));
                totalTime.add(time / 1000);
            }
            System.out.println("=============================");
            for (double time : totalTime) {
                System.out.println(time);
            }
        }
    }

    @Test
    void testScheduleAndRunTasksComparision() throws IOException {
        {
            int taskRunnerSize = 100;
            TaskDispatcher taskDispatcher = new TaskDispatcher("TD-Default", taskRunnerSize);
            FIFOScheduler fifoScheduler = new FIFOScheduler(taskDispatcher);
            // [SVM 137696.75, 143.073]
            for (int i = 0; i < applicationName.length; i++) {
                // first to test svm
                if(!(applicationName[i].contains("spark_strongly") || applicationName[i].contains("spark_pregel"))) {
                    continue;
                }
//                if(!applicationName[i].contains("spark_svm")) {
//                    continue;
//                }
                Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplication(fileName + applicationPath[i]);
                TaskGenerator.updateTaskTimeWithMaxTime(stageIdToTasks);
                Map<Long, Task> taskMap = TaskGenerator.generateTaskMap(stageIdToTasks);
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
                List<Double> totalTime = new ArrayList<>();
                for (int j = 1; j <= 200; j++) {
                    taskDispatcher = new TaskDispatcher("TD-D", j);
                    fifoScheduler.td = taskDispatcher;
                    double time = 0;
                    for (Job job : jobList) {
                        time += fifoScheduler.runTask(fifoScheduler.schedule(job.stages, stageIdToTasks),
                                false, taskMap);
                    }
                    totalTime.add(time / 1000);
                }
                System.out.println("=============================");
                for (double time : totalTime) {
                    System.out.println(time);
                }
            }

        }
    }

}