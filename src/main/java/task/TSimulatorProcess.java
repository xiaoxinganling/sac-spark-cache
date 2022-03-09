package task;


import entity.Job;
import entity.Partition;
import entity.RDD;
import entity.Task;
import org.apache.log4j.Logger;
import simulator.HotDataGenerator;
import simulator.JobGenerator;
import simulator.ReplacePolicy;
import task.scheduler.DAGAwareScheduler;
import task.scheduler.FIFOScheduler;
import task.scheduler.SimpleFIFOScheduler;
import task.scheduler.TaskScheduler;
import utils.NumberUtil;

import java.io.IOException;
import java.util.*;

public class TSimulatorProcess {

    private static Logger logger = Logger.getLogger(TSimulatorProcess.class);

    public static void processWithFIFOScheduleAndNoCache(String[] applicationNames, String[] fileNames) throws IOException {
        TaskDispatcher taskDispatcher = new TaskDispatcher("TD-FIFO-No-Cache", 100, null);
        SimpleFIFOScheduler simpleFifoScheduler = new SimpleFIFOScheduler(taskDispatcher);
        List<Double> totalTime = new ArrayList<>();
        for (int i = 0; i < applicationNames.length; i++) {
            List<Double> jobTimes = new ArrayList<>();
            if(!applicationNames[i].contains("spark_svm")) {
                continue;
            }
            Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(fileNames[i], null);
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileNames[i]);
            Map<Long, Task> taskMap = TaskGenerator.generateTaskMap(stageIdToTasks);
            double curApplicationTime = 0;
            for (Job job : jobList) {
                double curJobTime = simpleFifoScheduler.runTasks(simpleFifoScheduler.schedule(job.stages, stageIdToTasks),
                        false, taskMap);
                curApplicationTime += curJobTime;
                jobTimes.add(curJobTime / 1000);
            }
            totalTime.add(curApplicationTime / 1000);
            System.out.println(jobTimes);
        }
        System.out.println(totalTime);
        for (double t : totalTime) {
            System.out.println(t);
        }
    }

    public static void processWithFIFOScheduleAndCache(String[] applicationNames, String[] fileNames,
                                                       ReplacePolicy replacePolicy, int cacheSize,
                                                       int taskRunnerSize, double cacheSpaceRatio) throws IOException {
        SCacheSpace sCacheSpace = new SCacheSpace(cacheSize, replacePolicy);
        TaskDispatcher taskDispatcher = new TaskDispatcher("TD-FIFO-Cache", taskRunnerSize, sCacheSpace);
        SimpleFIFOScheduler simpleFifoScheduler = new SimpleFIFOScheduler(taskDispatcher);
        List<Double> totalTime = new ArrayList<>();
        for (int i = 0; i < applicationNames.length; i++) {
            List<Double> separateJobTime = new ArrayList<>();
//            if(!applicationNames[i].contains("spark_svm")) {
//                continue;
//            }
//            if(!applicationNames[i].contains("spark_strongly")) {
//                continue;
//            }
            System.out.println(String.format("measuring application [%s]...", applicationNames[i]));
            Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(fileNames[i], null);
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileNames[i]);
            Map<Long, Task> taskMap = TaskGenerator.generateTaskMap(stageIdToTasks);
            List<RDD> hotRDDs = HotDataGenerator.hotRDD(fileNames[i], jobList, null);
            long proposedSize = HotDataGenerator.proposeTaskCacheSpace(applicationNames[i], hotRDDs, stageIdToTasks);
            sCacheSpace.prepare(applicationNames[i], NumberUtil.numberWithRatio((int) proposedSize, cacheSpaceRatio), null, null, null);
            taskDispatcher.setHotRDDAndTaskRunnerHotInfo(hotRDDs, stageIdToTasks); // for TaskDispatcher
            double curApplicationTime = 0;
            for (Job job : jobList) {
                double curJobTime = simpleFifoScheduler.runTasks(simpleFifoScheduler.schedule(job.stages, stageIdToTasks), true, taskMap);
                curApplicationTime += curJobTime;
//                separateJobTime.add(curJobTime / 1000);
            }
            totalTime.add(curApplicationTime / 1000);
//            System.out.println(separateJobTime);
        }
        System.out.println(totalTime);
        for (double t : totalTime) {
            System.out.println(t);
        }
    }

    // KEYPOINT (replace policy, scheduler)
    public static void processWithNewScheduleAndCache(String[] applicationName, String[] fileNames,
                                                      ReplacePolicy replacePolicy, int taskRunnerSize,
                                                      double cacheSpaceRatio, ScheduleType scheduleType) throws IOException {
        SCacheSpace sCacheSpace = new SCacheSpace(1, replacePolicy);
        TaskDispatcher taskDispatcher = new TaskDispatcher("TD-Default", taskRunnerSize, sCacheSpace);
        TaskScheduler scheduler = new DAGAwareScheduler();
        if (scheduleType == ScheduleType.FIFO) {
            scheduler = new FIFOScheduler();
        }
        scheduler.td = taskDispatcher;
        List<Double> totalTime = new ArrayList<>();
        for (int i = 0; i < applicationName.length; i++) {
            // first to test svm
//            if(!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
//            if(!applicationName[i].contains("spark_strongly")) {
//                continue;
//            }
            Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(fileNames[i],
                    applicationName[i]);
            scheduler.setStageIdToTasksAndTaskSizeToRun(stageIdToTasks);
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileNames[i]);
            List<RDD> hotRDDs = HotDataGenerator.hotRDD(applicationName[i], jobList, null);
            long proposedSize = HotDataGenerator.proposeTaskCacheSpace(applicationName[i], hotRDDs, stageIdToTasks);
            taskDispatcher.setHotRDDAndTaskRunnerHotInfo(hotRDDs, stageIdToTasks);
            // get hot partitions
            Set<Long> hotRDDIdSet = new HashSet<>();
            for (RDD rdd : hotRDDs) {
                hotRDDIdSet.add(rdd.rddId);
            }
            Map<String, Partition> hotPartitionMap = HotDataGenerator.generateHotPartitionMap(hotRDDIdSet, stageIdToTasks);
            List<Partition> hotPartitions = new ArrayList<>(hotPartitionMap.values());
            // end get
            sCacheSpace.prepare(applicationName[i], NumberUtil.numberWithRatio((int) proposedSize, cacheSpaceRatio),
                    jobList, stageIdToTasks, hotPartitions);
            // need to update job list
            JobGenerator.updateStageCPUResourceOfJobList(jobList, applicationName[i]);
            double totalAppTime = 0;
            List<Double> separateJobTime = new ArrayList<>();
            for (Job job : jobList) {
                scheduler.initStageTaskInfoPerJob(job.stages);
                taskDispatcher.resetAccumulateTimes();
                double curJobTime = taskDispatcher.runAtTaskRunnersWithOneJobAndCache(scheduler, sCacheSpace);
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



}
