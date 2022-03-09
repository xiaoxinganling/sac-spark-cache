package task.scheduler;

import entity.Stage;
import entity.TSDecision;
import entity.Task;
import task.TaskDispatcher;

import java.util.*;

public abstract class TaskScheduler {

    // 用于记录Stage已完成的task数
    public Map<Long, Integer> stageTasksToRun;

    // 用于调度task、调用TaskRunner执行task
    public TaskDispatcher td;

    // 单个job的stage map
    public Map<Long, Stage> stageMap;

    // 单个job的未完成stage id
    public Set<Long> unFinishedStageIds;

    // 单个job中每个stage的后续stage id
    public Map<Long, List<Long>> stageIdToAdjStageIds;

    // 多个job的stage id 与 task list映射表
    public Map<Long, List<Task>> stageIdToTasks;

    // 单个job待提交的task优先权队列
    public PriorityQueue<Task> taskMaxHeap;

    public TaskScheduler() {
    }

    public TaskScheduler(TaskDispatcher td) {
        this.td = td;
    }

    public TaskScheduler(TaskDispatcher td, Map<Long, List<Task>> stageIdToTasks) {
        this.td = td;
        setStageIdToTasksAndTaskSizeToRun(stageIdToTasks);
    }

    public void setStageIdToTasksAndTaskSizeToRun(Map<Long, List<Task>> stageIdToTasks) {
        this.stageIdToTasks = stageIdToTasks;
        stageTasksToRun = new HashMap<>();
        for (Map.Entry<Long, List<Task>> entry : stageIdToTasks.entrySet()) {
            stageTasksToRun.put(entry.getKey(), entry.getValue().size());
        }
    }

    // List<TSDecision>中的Task并行分配到各个TaskRunner,各个List之间时间互相累加
    public abstract List<List<TSDecision>> schedule(List<Stage> stages, Map<Long, List<Task>> stageIdToTasks);

    /**
     * run task
     * @param scheduleSlotList schedule策略
     * @param withCache 是否使用cache
     * @param taskMap Task哈希表
     * @return
     */
    public abstract double runTasks(List<List<TSDecision>> scheduleSlotList, boolean withCache, Map<Long, Task> taskMap);

    /**
     * 根据不同的scheduler初始化最小堆
     */
    public abstract void createAndInitMaxHeap();

    public abstract void initPriorityOfStages();

    public boolean stageIsFinished(Stage curStage, Task curTask) {
//        return curTask.getIndexInStage() == curStage.taskNum - 1;
        int curTaskNum = stageTasksToRun.get(curStage.stageId);
        curTaskNum -= 1;
        stageTasksToRun.put(curStage.stageId, curTaskNum);
        if (curTaskNum <= 0) {
            assert curTaskNum == 0;
            return true;
        }
        return false;
    }

    public List<Task> provideTasksUnderResourceLimit(List<Task> finishedTask, int idleCPU) {
        // 1. update taskMinHeap
        for (Task task : finishedTask) {
            Stage curStage = stageMap.get(task.stageId);
            if (stageIsFinished(curStage, task)) {
                // last task
                unFinishedStageIds.remove(curStage.stageId);
                List<Long> adjStageIds = stageIdToAdjStageIds.get(curStage.stageId);
                if (adjStageIds != null) {
                    for (long adjStageId : adjStageIds) {
                        boolean toAdd = true;
                        Stage stageToAdd = stageMap.get(adjStageId);
                        for (long parentId :stageToAdd.parentIDs) {
                            if (stageMap.containsKey(parentId) && unFinishedStageIds.contains(parentId)) {
                                toAdd = false;
                                break;
                            }
                        }
                        if (toAdd) {
                            for (Task t : stageIdToTasks.get(adjStageId)) {
                                taskMaxHeap.offer(t);
                            }
                        }
                    }
                }
            }
        }
        // 2. add list to schedule
        List<Task> toSchedule = new ArrayList<>();
        while (!taskMaxHeap.isEmpty() && idleCPU >= taskMaxHeap.peek().getNeedCPU()) {
            Task toScheduleTask = taskMaxHeap.poll();
            toSchedule.add(toScheduleTask);
            assert toScheduleTask != null;
            idleCPU -= toScheduleTask.getNeedCPU();
        }
        return toSchedule;
    }

    // call for each job
    public void initStageTaskInfoPerJob(List<Stage> stages) {
        // 1. init stage map
        stageMap = new HashMap<>();
        for (Stage stage : stages) {
            stageMap.put(stage.stageId, stage);
        }
        // 2. init stage's adj stage ids
        stageIdToAdjStageIds = new HashMap<>();
        for (Stage stage : stages) {
            for (long parentId : stage.parentIDs) {
                if (stageMap.containsKey(parentId)) {
                    List<Long> subsequent = stageIdToAdjStageIds.getOrDefault(parentId,
                            new ArrayList<>());
                    subsequent.add(stage.stageId);
                    stageIdToAdjStageIds.put(parentId, subsequent);
                }
            }
        }
        // 3 init unfinished stage ids
        unFinishedStageIds = new HashSet<>(stageMap.keySet());
        // 4. update tasks stage priority
        initPriorityOfStages();
        // 5. create min heap and init it with adding some task
        createAndInitMaxHeap();
    }

    public void firstInitMaxHeap() {
        for (Stage stage : stageMap.values()) {
            boolean needToAdd = true;
            for (long parentId : stage.parentIDs) {
                if (stageMap.containsKey(parentId) && unFinishedStageIds.contains(parentId)) {
                    needToAdd = false;
                    break;
                }
            }
            if (needToAdd) {
                List<Task> tasks = stageIdToTasks.get(stage.stageId);
                for (Task t : tasks) {
                    taskMaxHeap.offer(t);
                }
            }
        }
    }

}
