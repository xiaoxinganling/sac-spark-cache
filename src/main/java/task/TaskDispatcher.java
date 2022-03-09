package task;

import entity.*;
import org.apache.log4j.Logger;
import simulator.HotDataGenerator;
import simulator.JobStageSubmitter;
import task.scheduler.TaskScheduler;
import java.io.IOException;
import java.util.*;

public class TaskDispatcher {

    public static final Long TASK_FINISH_TIME_SCHEDULE = Long.MAX_VALUE - 4;

    public String taskDispatcherId;

    private TaskRunner[] taskRunners;

    private double[] accumulateTimes;

    private Logger logger = Logger.getLogger(this.getClass());

    private SCacheSpace sCacheSpace;

    private List<RDD> hotRDD;

    public int getActiveRunners() {
        return activeRunners;
    }

    public void setActiveRunners(int activeRunners) {
        this.activeRunners = activeRunners;
    }

    private int activeRunners;

    public List<RDD> getHotRDD() {
        return hotRDD;
    }

    public void setHotRDDAndTaskRunnerHotInfo(List<RDD> hotRDD, Map<Long, List<Task>> stageIdToTasks) {
        this.hotRDD = hotRDD;
        Set<Long> hotRDDIdSet = new HashSet<>();
        for (RDD rdd : hotRDD) {
            hotRDDIdSet.add(rdd.rddId);
        }
        Map<String, Partition> hotPartitionMap = HotDataGenerator.generateHotPartitionMap(hotRDDIdSet, stageIdToTasks);
        for (TaskRunner tr : taskRunners) {
            tr.setHotRDDIdSet(hotRDDIdSet);
            tr.setHotPartitionMap(hotPartitionMap);
        }
    }

    public void setsCacheSpace(SCacheSpace sCacheSpace) {
        this.sCacheSpace = sCacheSpace;
    }

    public TaskDispatcher(String taskDispatcherId, int taskRunnerSize, SCacheSpace sCacheSpace) {
        logger.info(String.format("TaskDispatcher [%s] is created with [%d] TaskRunner.",
                taskDispatcherId, taskRunnerSize));
        this.taskDispatcherId = taskDispatcherId;
        taskRunners = new TaskRunner[taskRunnerSize];
        accumulateTimes = new double[taskRunnerSize];
        for (int i = 0; i < taskRunnerSize; i++) {
            taskRunners[i] = new TaskRunner(String.format("%s-T-R-%d", taskDispatcherId, i), 1, 1);
        }
        setsCacheSpace(sCacheSpace);
        activeRunners = taskRunnerSize;
    }

    public double dispatchAndRunTask(String applicationName, String applicationPath, Map<Long, List<Task>> stageIdToTasks) throws IOException {
//        StringBuilder sb = new StringBuilder();
        Map<Long, Task> taskMap = TaskGenerator.generateTaskMap(stageIdToTasks);
        JobStageSubmitter jss = new JobStageSubmitter(applicationName, applicationPath);
//        Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplication(applicationPath);
//        TaskGenerator.updateTaskTimeWithMaxTime(stageIdToTasks);
        double applicationTotalTime = 0;
        for (Job job : jss.jobList) {
            // T√ODO: need to remove
//            for (Stage stage : job.stages) {
//                List<Task> tasks = stageIdToTasks.get(stage.stageId);
//                for (Task task : tasks) {
//                    task.setDuration(stage.completeTime - stage.submitTime);
//                }
//            }
            // end T√ODO
            double jobTotalTime = 0;
            List<Stage> tmp = jss.submitAvailableJob();
            List<TSDecision> tsDecisions = dispatchTask(stageIdToTasks, tmp);
            double curTime = runTasks(tsDecisions, taskMap, false);
            jobTotalTime += curTime;
//            sb.append(tmp.get(0).stageId).append(":").append(curTime).append("\n");
            logger.info(String.format("TaskDispatcher [%s] dispatch tasks in [%d] Stages, first Stage id: [%d], and run for [%f] ms.",
                    taskDispatcherId, tmp.size(), tmp.get(0).stageId, curTime));
            List<Stage> toSubmit;
            while ((toSubmit = jss.submitAvailableStages()) != null) {
                tsDecisions = dispatchTask(stageIdToTasks, toSubmit);
                curTime = runTasks(tsDecisions, taskMap, false);
                jobTotalTime += curTime;
//                sb.append(toSubmit.get(0).stageId).append(":").append(curTime).append("\n");
                logger.info(String.format("TaskDispatcher [%s] dispatch tasks in [%d] Stages, first Stage id: [%d], and run for [%f] ms.",
                        taskDispatcherId, toSubmit.size(), toSubmit.get(0).stageId, curTime));
            }
            applicationTotalTime += jobTotalTime;
        }
//        System.out.println(sb.toString());
        return applicationTotalTime;
    }

    // dispatch一次task就run一次task

    /**
     * 调度一批可并行stage中的task
     * @param stageIdToTasks
     * @param submittedStage
     */
    public List<TSDecision> dispatchTask(Map<Long, List<Task>> stageIdToTasks, List<Stage> submittedStage) {
        List<TSDecision> decisions = new ArrayList<>();
        List<Long> stageIds = new LinkedList<>();
        for (Stage stage : submittedStage) {
            stageIds.add(stage.stageId);
        }
//        Collections.sort(stageIds); // TO√DO: 可以不排序
        int taskRunnerIndex = 0, taskQueueIndex = 0;
        for (long stageId : stageIds) {
            List<Task> tasks = stageIdToTasks.get(stageId);
            tasks.sort((o1, o2) -> (int) (o1.getTaskId() - o2.getTaskId())); // sort task
            // for log
            List<Long> taskIds = new ArrayList<>();
            for (Task task : tasks) {
                taskIds.add(task.getTaskId());
            }
            // end for log
            logger.info(String.format("StageDispatcher [%s] dispatches Tasks %s to TaskRunner.",
                    taskDispatcherId, taskIds));
            for (Task task : tasks) {
//                taskRunners[taskRunnerIndex].receiveTask(task, taskQueueIndex);
                decisions.add(new TSDecision(task.getTaskId(),
                        taskRunnerIndex, taskQueueIndex));
                taskQueueIndex++;
                if (taskRunners[taskRunnerIndex].coreNum == taskQueueIndex) {
                    taskRunnerIndex = (taskRunnerIndex + 1) % taskRunners.length;
                    taskQueueIndex = 0;
                }
            }
        }
        return decisions;
    }

    public double runTasks(List<TSDecision> taskScheduleDecision, Map<Long, Task> taskMap, boolean withCache) {
        for (TSDecision tsDecision : taskScheduleDecision) {
            taskRunners[tsDecision.taskRunnerIndex].receiveTask(taskMap.get(tsDecision.taskId),
                    tsDecision.queueIndex);
        }
//        logger.info(String.format("TaskDispatcher [%s] is instructing TaskRunner to run Tasks.", taskDispatcherId));
        if (withCache) {
            List<List<Double>> taskRunnerQueueTime = new ArrayList<>();
            for (int i = 0; i < taskRunners.length; i++) {
                taskRunnerQueueTime.add(new ArrayList<>());
                for (int j = 0; j < taskRunners[i].getTaskQueueList().size(); j++) {
                    taskRunnerQueueTime.get(i).add(0.0);
                }
            }
            int curTaskRunnerIndex = 0, curQueueIndex = 0, totalTask = taskScheduleDecision.size();
            while (totalTask > 0) {
                double time = taskRunners[curTaskRunnerIndex].runOneTaskWithSCacheSpace(curQueueIndex, sCacheSpace);
                if (time != TaskRunner.NO_TASK_TIME) {
                    totalTask--;
                    double newTime = taskRunnerQueueTime.get(curTaskRunnerIndex).get(curQueueIndex) + time;
                    taskRunnerQueueTime.get(curTaskRunnerIndex).set(curQueueIndex, newTime);
                }
                curQueueIndex++;
                if (curQueueIndex == taskRunners[curTaskRunnerIndex].getTaskQueueList().size()) {
                    curTaskRunnerIndex = (curTaskRunnerIndex + 1) % taskRunners.length; // 这里是头咬住尾巴
                    curQueueIndex = 0;
                }
            }
            double res = 0;
            for (int i = 0; i < taskRunnerQueueTime.size(); i++) {
                List<Double> eachTaskRunner = taskRunnerQueueTime.get(i);
                double max = 0;
                for (double eachQueue : eachTaskRunner) {
                    max = Math.max(eachQueue, max);
                }
                if (max != 0) {
                    logger.info(String.format("TaskDisPatcher [%s] call TaskRunner [%s] run tasks for [%f]ms.",
                            taskDispatcherId, taskRunners[i].taskRunnerId, max));
                }
                res = Math.max(res, max);
            }
            return res;
        }
        double lastTime = 0;
        for (TaskRunner taskRunner : taskRunners) {
            if (taskRunner.totalTaskSize() == 0) {
                continue;
            }
            double queueTime = taskRunner.runTasks();
            lastTime = Math.max(lastTime, queueTime);
            logger.info(String.format("TaskDispatcher [%s] run tasks at TaskRunner [%s] for [%f] ms.",
                    taskDispatcherId, taskRunner.taskRunnerId, queueTime));
        }
        return lastTime;
    }

    // call for each job
    public void resetAccumulateTimes() {
        Arrays.fill(accumulateTimes, 0);
    }

    // 默认initial timeline为0，job之间的time外面处理
    // call for each job
    public Map<Long, Double> scheduleAtTaskRunnersWithOneJob(TaskScheduler taskScheduler) {
        double curTimeLine = 0;
        Map<Long, Double> taskStartTimeMap = new HashMap<>();
        while (true) {
            List<Task> finishedTasks = new ArrayList<>();
            Set<Long> finishedTaskIds = new HashSet<>();
            // 1. add finished task
            int testNeedCPU = 0;
            for (int i = 0; i < taskRunners.length; i++) {
                if (taskRunners[i].totalTaskSize() > 0 &&
                        curTimeLine >= accumulateTimes[i] + taskRunners[i].getOneTaskWithOnlyOneQueue().getDuration()) {
                    Task curTask = taskRunners[i].pollOneTaskWithOnlyOneQueue();
                    activeRunners += 1;
                    testNeedCPU += 1;
                    // 其实永远是相等的
                    double accumulateTime = accumulateTimes[i] + curTask.getDuration();
                    assert accumulateTime == curTimeLine;
                    // end 其实永远是相等的
                    if (!finishedTaskIds.contains(curTask.getTaskId())) {
                        finishedTasks.add(curTask);
                        finishedTaskIds.add(curTask.getTaskId());
                    }
                }
            }
            // assert needCPU works well
            for (Task finishedTask : finishedTasks) {
                testNeedCPU -= finishedTask.getNeedCPU();
            }
            assert testNeedCPU == 0;
            // end check
            // 2. update accumulated time of empty TaskRunner
            if (curTimeLine > 0) {
                for (int i = 0; i < taskRunners.length; i++) {
                    if (taskRunners[i].totalTaskSize() == 0) {
                        accumulateTimes[i] = curTimeLine;
                    }
                }
            }
            // 3. ask scheduler to provide task list
            List<Task> taskToSchedule = taskScheduler.provideTasksUnderResourceLimit(finishedTasks, activeRunners);
            // return if the stage to schedule is finished
            if (taskToSchedule.size() == 0 && activeRunners == taskRunners.length) {
                // need to return runtime of stages in this job
                taskStartTimeMap.put(TASK_FINISH_TIME_SCHEDULE, curTimeLine);
                break;
            }
            // 4. schedule tasks (optimal) and update curTimeLine
            curTimeLine = Double.MAX_VALUE;
            for (Task task : taskToSchedule) {
                int curTaskCPUNeed = task.getNeedCPU();
                for (int i = 0; i < taskRunners.length; i++) {
                    TaskRunner taskRunner = taskRunners[i];
                    if (curTaskCPUNeed == 0) {
                        break;
                    }
                    if (taskRunner.totalTaskSize() == 0) {
                        taskRunner.receiveTask(task, 0);
                        if (taskStartTimeMap.containsKey(task.getTaskId())) {
                            assert taskStartTimeMap.get(task.getTaskId()) == accumulateTimes[i];
                        } else {
                            taskStartTimeMap.put(task.getTaskId(), accumulateTimes[i]);
                        }
                        activeRunners -= 1;
                        curTaskCPUNeed -= 1;
                    }
                }
            }
            Set<Long> taskConsidered = new HashSet<>();
            for (int i = 0; i < taskRunners.length; i++) {
                if (taskRunners[i].totalTaskSize() > 0) {
                    assert taskRunners[i].totalTaskSize() == 1;
                    Task curTask = taskRunners[i].getOneTaskWithOnlyOneQueue();
                    if (taskConsidered.contains(curTask.getTaskId())) {
                        continue;
                    }
                    // update curTimeline
                    curTimeLine = Math.min(curTimeLine, accumulateTimes[i] + curTask.getDuration().intValue());
                    taskConsidered.add(curTask.getTaskId());
                }
            }
        }
        return taskStartTimeMap;
    }

    // 有些粗浅，后续有空再改正
    // task start time可以给cache用（不太现实）
    // call for each job
    public double runAtTaskRunnersWithOneJobAndCache(TaskScheduler taskScheduler, SCacheSpace sCacheSpace) {
        double curTimeLine = 0;
        Map<Long, Double> taskIdToRunTimeUnderCache = new HashMap<>();
        Map<Long, Double> taskStartTimeMap = new HashMap<>();
        while (true) {
            List<Task> finishedTasks = new ArrayList<>();
            Set<Long> finishedTaskIds = new HashSet<>();
            // 1. add finished task
            int testNeedCPU = 0;
            for (int i = 0; i < taskRunners.length; i++) {
                if (taskRunners[i].totalTaskSize() > 0 &&
                        curTimeLine >= accumulateTimes[i] + taskIdToRunTimeUnderCache.get(taskRunners[i].getOneTaskWithOnlyOneQueue().getTaskId())) {
                    // 只要被调度过，就肯定会有该task的run time
                    Task curTask = taskRunners[i].pollOneTaskWithOnlyOneQueue();
                    activeRunners += 1;
                    testNeedCPU += 1;
                    // 其实永远是相等的
                    double accumulateTime = accumulateTimes[i] + taskIdToRunTimeUnderCache.get(curTask.getTaskId());
                    assert accumulateTime == curTimeLine;
                    // end 其实永远是相等的
                    if (!finishedTaskIds.contains(curTask.getTaskId())) {
                        finishedTasks.add(curTask);
                        finishedTaskIds.add(curTask.getTaskId());
                    }
                }
            }
            // assert needCPU works well
            for (Task finishedTask : finishedTasks) {
                testNeedCPU -= finishedTask.getNeedCPU();
            }
            assert testNeedCPU == 0;
            // end check
            // 2. update accumulated time of empty TaskRunner
            if (curTimeLine > 0) {
                for (int i = 0; i < taskRunners.length; i++) {
                    if (taskRunners[i].totalTaskSize() == 0) {
                        // 理论上来说无需取max
//                        System.out.println(String.format("%f--%f", accumulateTimes[i], curTimeLine));
                        assert accumulateTimes[i] <= curTimeLine; // 要么小于要么等于
                        accumulateTimes[i] = curTimeLine;
                    }
                }
            }
            // 3. ask scheduler to provide task list
            List<Task> taskToSchedule = taskScheduler.provideTasksUnderResourceLimit(finishedTasks, activeRunners);
            // return if the stage to schedule is finished
            if (taskToSchedule.size() == 0 && activeRunners == taskRunners.length) {
                // need to return runtime of stages in this job
                taskStartTimeMap.put(TASK_FINISH_TIME_SCHEDULE, curTimeLine);
                break;
            }
            // 4. schedule tasks (optimal)
            curTimeLine = Double.MAX_VALUE;
            for (Task task : taskToSchedule) {
                int curTaskCPUNeed = task.getNeedCPU();
                for (int i = 0; i < taskRunners.length; i++) {
                    TaskRunner taskRunner = taskRunners[i];
                    if (curTaskCPUNeed == 0) {
                        break;
                    }
                    if (taskRunner.totalTaskSize() == 0) {
                        taskRunner.receiveTask(task, 0);
                        if (taskStartTimeMap.containsKey(task.getTaskId())) {
                            assert taskStartTimeMap.get(task.getTaskId()) == accumulateTimes[i];
                        } else {
                            taskStartTimeMap.put(task.getTaskId(), accumulateTimes[i]);
                        }
                        activeRunners -= 1;
                        curTaskCPUNeed -= 1;
                    }
                }
            }
            // 5.update curTimeLine
            Set<Long> taskHasRun = new HashSet<>();
            for (int i = 0; i < taskRunners.length; i++) {
                if (taskRunners[i].totalTaskSize() > 0) {
                    assert taskRunners[i].totalTaskSize() == 1;
                    Task curTask = taskRunners[i].getOneTaskWithOnlyOneQueue();
                    // KEYPOINT: 避免重复运算！！！
                    if (taskHasRun.contains(curTask.getTaskId())) {
                        continue;
                    }
                    // run task and update curTimeline
                    // update curTimeline
                    // 这里重复计算了
                    if (taskIdToRunTimeUnderCache.containsKey(curTask.getTaskId())) {
                        curTimeLine = Math.min(curTimeLine, accumulateTimes[i] +
                                taskIdToRunTimeUnderCache.get(curTask.getTaskId()));
                    } else {
                        double taskRuntimeWithCache = taskRunners[i].runOneTaskWithSCacheSpace(0, sCacheSpace);
                        curTimeLine = Math.min(curTimeLine, accumulateTimes[i] + taskRuntimeWithCache);
                        // 因为poll了task，还得把它塞回去
                        taskRunners[i].receiveTask(curTask, 0);
                        taskIdToRunTimeUnderCache.put(curTask.getTaskId(), taskRuntimeWithCache);
                    }
                    taskHasRun.add(curTask.getTaskId());
                }
            }
        }
        // 6. return run time
        assert taskIdToRunTimeUnderCache.size() == taskStartTimeMap.size() - 1; // add了一个MAX_VALUE
        double res = 0;
        for (Map.Entry<Long, Double> entry : taskStartTimeMap.entrySet()) {
            if (entry.getKey() != TASK_FINISH_TIME_SCHEDULE) {
                long taskId = entry.getKey();
                res = Math.max(res, entry.getValue() + taskIdToRunTimeUnderCache.get(taskId));
            }
        }
        assert taskStartTimeMap.get(TASK_FINISH_TIME_SCHEDULE) == res;
        return res;
    }

}
