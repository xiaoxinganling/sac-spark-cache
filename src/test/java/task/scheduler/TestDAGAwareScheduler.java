package task.scheduler;

import entity.Job;
import entity.RDD;
import entity.Task;
import org.junit.jupiter.api.Test;
import simulator.HotDataGenerator;
import simulator.JobGenerator;
import simulator.ReplacePolicy;
import sketch.StaticSketch;
import task.SCacheSpace;
import task.TaskDispatcher;
import task.TaskGenerator;
import utils.NumberUtil;
import java.io.IOException;
import java.util.*;

class TestDAGAwareScheduler {

    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    String fileName = "E:\\Google Chrome Download\\";
    int taskRunnerSize = 40;

    @Test
    void testDAGAwareScheduleWithOutCache() throws IOException {
        TaskDispatcher taskDispatcher = new TaskDispatcher("TD-Default", taskRunnerSize, null);
        DAGAwareScheduler dagAwareScheduler = new DAGAwareScheduler(taskDispatcher);
        List<Double> totalTime = new ArrayList<>();
        for (int i = 0; i < applicationName.length; i++) {
            // first to test svm
//            if(!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
            if(!applicationName[i].contains("spark_strongly")) {
                continue;
            }
            Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(fileName + applicationPath[i],
                    applicationName[i]);
            dagAwareScheduler.setStageIdToTasksAndTaskSizeToRun(stageIdToTasks);
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            // need to update job list
            JobGenerator.updateStageCPUResourceOfJobList(jobList, applicationName[i]);
            double lastJobTime = 0;
            Map<Long, Double> totalStartTimeOfTask = new HashMap<>();
            for (Job job : jobList) {
                dagAwareScheduler.initStageTaskInfoPerJob(job.stages);
                taskDispatcher.resetAccumulateTimes();
                Map<Long, Double> startTimeOfTask = taskDispatcher.scheduleAtTaskRunnersWithOneJob(dagAwareScheduler);
                for (Map.Entry<Long, Double> entry : startTimeOfTask.entrySet()) {
                    assert !totalStartTimeOfTask.containsKey(entry.getKey());
                    if (!entry.getKey().equals(TaskDispatcher.TASK_FINISH_TIME_SCHEDULE)) {
                        totalStartTimeOfTask.put(entry.getKey(), lastJobTime + entry.getValue());
                    }
                }
                lastJobTime += startTimeOfTask.get(TaskDispatcher.TASK_FINISH_TIME_SCHEDULE);
            }
            System.out.println(totalStartTimeOfTask);
            List<Long> keyList = new ArrayList<>(totalStartTimeOfTask.keySet());
//            keyList.sort((o1, o2) -> (int) (o1 - o2));
            for (long key : keyList) {
//                System.out.println(String.format("task: %d, start time: %f", key, totalStartTimeOfTask.get(key)));
                System.out.println(totalStartTimeOfTask.get(key));
            }
            System.out.println("======" + totalStartTimeOfTask.size());
            for (int j = 0; j < totalStartTimeOfTask.size(); j++) {
                System.out.println(j);
            }
        }
    }

    @Test
    void testFIFOScheduleWithoutCache() throws IOException {
        TaskDispatcher taskDispatcher = new TaskDispatcher("TD-Default", taskRunnerSize, null);
        FIFOScheduler fifoScheduler = new FIFOScheduler(taskDispatcher);
        List<Double> totalTime = new ArrayList<>();
        for (int i = 0; i < applicationName.length; i++) {
            // first to test svm
//            if(!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
            if(!applicationName[i].contains("spark_strongly")) {
                continue;
            }
            Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(fileName + applicationPath[i],
                    applicationName[i]);
            fifoScheduler.setStageIdToTasksAndTaskSizeToRun(stageIdToTasks);
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            double lastJobTime = 0;
            Map<Long, Double> totalStartTimeOfTask = new HashMap<>();
            for (Job job : jobList) {
                fifoScheduler.initStageTaskInfoPerJob(job.stages);
                taskDispatcher.resetAccumulateTimes();
                Map<Long, Double> startTimeOfTask = taskDispatcher.scheduleAtTaskRunnersWithOneJob(fifoScheduler);
                for (Map.Entry<Long, Double> entry : startTimeOfTask.entrySet()) {
                    assert !totalStartTimeOfTask.containsKey(entry.getKey());
                    if (!entry.getKey().equals(TaskDispatcher.TASK_FINISH_TIME_SCHEDULE)) {
                        totalStartTimeOfTask.put(entry.getKey(), lastJobTime + entry.getValue());
                    }
                }
                lastJobTime += startTimeOfTask.get(TaskDispatcher.TASK_FINISH_TIME_SCHEDULE);
            }
            System.out.println(totalStartTimeOfTask);
            List<Long> keyList = new ArrayList<>(totalStartTimeOfTask.keySet());
            keyList.sort((o1, o2) -> (int) (o1 - o2));
            for (long key : keyList) {
                System.out.println(totalStartTimeOfTask.get(key));
            }
            System.out.println("======" + totalStartTimeOfTask.size());
            for (int j = 0; j < totalStartTimeOfTask.size(); j++) {
                System.out.println(j);
            }
        }
    }

    @Test
    void testDAGAwareScheduleWithCache() throws IOException {
        // 参考TSimulatorProcess.java
        SCacheSpace sCacheSpace = new SCacheSpace(1, ReplacePolicy.SLRU);
        TaskDispatcher taskDispatcher = new TaskDispatcher("TD-Default", taskRunnerSize, sCacheSpace);
        DAGAwareScheduler dagAwareScheduler = new DAGAwareScheduler(taskDispatcher);
        List<Double> totalTime = new ArrayList<>();
        for (int i = 0; i < applicationName.length; i++) {
            // first to test svm
//            if(!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
//            if(!applicationName[i].contains("spark_strongly")) {
//                continue;
//            }
            Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(fileName + applicationPath[i],
                    applicationName[i]);
            dagAwareScheduler.setStageIdToTasksAndTaskSizeToRun(stageIdToTasks);
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            List<RDD> hotRDDs = HotDataGenerator.hotRDD(applicationName[i], jobList, null);
            long proposedSize = HotDataGenerator.proposeTaskCacheSpace(applicationName[i], hotRDDs, stageIdToTasks);
            sCacheSpace.prepare(applicationName[i], NumberUtil.numberWithRatio((int) proposedSize, 0.2), null, null, null);
            taskDispatcher.setHotRDDAndTaskRunnerHotInfo(hotRDDs, stageIdToTasks);
            // need to update job list
            JobGenerator.updateStageCPUResourceOfJobList(jobList, applicationName[i]);
            double totalAppTime = 0;
            List<Double> separateJobTime = new ArrayList<>();
            for (Job job : jobList) {
                dagAwareScheduler.initStageTaskInfoPerJob(job.stages);
                taskDispatcher.resetAccumulateTimes();
                double curJobTime = taskDispatcher.runAtTaskRunnersWithOneJobAndCache(dagAwareScheduler, sCacheSpace);
                separateJobTime.add(curJobTime);
                totalAppTime += curJobTime;
            }
            System.out.println(separateJobTime);
            totalTime.add(totalAppTime / 1000);
        }
        System.out.println(totalTime);
        for (double t : totalTime) {
            System.out.println(t);
        }
    }

    @Test
    void testFIFOScheduleWithCache() throws IOException {
        SCacheSpace sCacheSpace = new SCacheSpace(1, ReplacePolicy.SLRU);
        TaskDispatcher taskDispatcher = new TaskDispatcher("TD-Default", taskRunnerSize, sCacheSpace);
        FIFOScheduler fifoScheduler = new FIFOScheduler(taskDispatcher);
        List<Double> totalTime = new ArrayList<>();
        for (int i = 0; i < applicationName.length; i++) {
            // first to test svm
//            if(!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
//            if(!applicationName[i].contains("spark_strongly")) {
//                continue;
//            }
            Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(fileName + applicationPath[i],
                    applicationName[i]);
            fifoScheduler.setStageIdToTasksAndTaskSizeToRun(stageIdToTasks);
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            List<RDD> hotRDDs = HotDataGenerator.hotRDD(applicationName[i], jobList, null);
            long proposedSize = HotDataGenerator.proposeTaskCacheSpace(applicationName[i], hotRDDs, stageIdToTasks);
            sCacheSpace.prepare(applicationName[i], NumberUtil.numberWithRatio((int) proposedSize, 0.2), null, null, null);
            taskDispatcher.setHotRDDAndTaskRunnerHotInfo(hotRDDs, stageIdToTasks);
            // need to update job list
            JobGenerator.updateStageCPUResourceOfJobList(jobList, applicationName[i]);
            double totalAppTime = 0;
            List<Double> separateJobTime = new ArrayList<>();
            for (Job job : jobList) {
                fifoScheduler.initStageTaskInfoPerJob(job.stages);
                taskDispatcher.resetAccumulateTimes();
                double curJobTime = taskDispatcher.runAtTaskRunnersWithOneJobAndCache(fifoScheduler, sCacheSpace);
                separateJobTime.add(curJobTime);
                totalAppTime += curJobTime;
            }
            System.out.println(separateJobTime);
            totalTime.add(totalAppTime / 1000);
        }
        System.out.println(totalTime);
        for (double t : totalTime) {
            System.out.println(t);
        }
    }

    @Test
    void testFIFOAndSDAGAwareSchedulerWithCache() {

    }

}