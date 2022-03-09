package task.scheduler;

import entity.Stage;
import entity.TSDecision;
import entity.Task;
import task.TaskDispatcher;

import java.util.*;

public class DAGAwareScheduler extends TaskScheduler {

    // 单个job
//    private Map<Long, Stage> stageMap;

    // 单个job
//    private Set<Long> unFinishedStageIds;

    // 多个job
//    private Map<Long, List<Task>> stageIdToTasks;

    // 多个job
    // private Map<Long, Integer> stageTasksToRun;

    // 单个job
//    private PriorityQueue<Task> taskMinHeap;

    // 单个job
//    private Map<Long, List<Long>> stageIdToAdjStageIds;

    // call for only once


    public DAGAwareScheduler() {
    }

    public DAGAwareScheduler(TaskDispatcher td) {
        super(td);
    }

    public DAGAwareScheduler(TaskDispatcher td, Map<Long, List<Task>> stageIdToTasks) {
        super(td, stageIdToTasks);
    }

    // call for each job
//    public void initStageTaskInfoPerJob(List<Stage> stages) {
//        // 1. init stage map
//        stageMap = new HashMap<>();
//        for (Stage stage : stages) {
//            stageMap.put(stage.stageId, stage);
//        }
//        // 2. init stage's adj stage ids
//        stageIdToAdjStageIds = new HashMap<>();
//        for (Stage stage : stages) {
//            for (long parentId : stage.parentIDs) {
//                if (stageMap.containsKey(parentId)) {
//                    List<Long> subsequent = stageIdToAdjStageIds.getOrDefault(parentId,
//                            new ArrayList<>());
//                    subsequent.add(stage.stageId);
//                    stageIdToAdjStageIds.put(parentId, subsequent);
//                }
//            }
//        }
//        // 3 init unfinished stage ids
//        unFinishedStageIds = new HashSet<>(stageMap.keySet());
//        // 4. update tasks stage priority
//        Map<Long, Integer> onlyAdjSP = new HashMap<>();
//        for (Map.Entry<Long, List<Task>> entry : stageIdToTasks.entrySet()) {
//            Stage curStage = stageMap.get(entry.getKey());
//            if (curStage == null) {
//                continue;
//            } // stage map是单个job的
//            int otherSP = generateSchedulePriorityOfStage(curStage, onlyAdjSP);
//            List<Task> tasks = entry.getValue();
//            int curSp = otherSP;
//            for (int i = tasks.size() - 1; i >= 0; i--) {
//                Task curTask = tasks.get(i);
//                curSp += curTask.getNeedCPU() * curTask.getDuration();
//                curTask.setSchedulePriority(curSp);
//            }
//        }
//        createAndInitMinHeap();
//    }

    // backtrack + memory map
    public int generateSchedulePriorityOfStage(Stage stage, Map<Long, Integer> onlyAdjSchedulerPriority) {
        List<Long> adjIds = stageIdToAdjStageIds.get(stage.stageId);
        if (adjIds == null || adjIds.size() == 0) {
            return 0;
        }
        if (onlyAdjSchedulerPriority.containsKey(stage.stageId)) {
            return onlyAdjSchedulerPriority.get(stage.stageId);
        }
        List<Task> tasks = stageIdToTasks.get(stage.stageId);
        assert tasks.size() > 0;
        int res = (int) (tasks.size() * tasks.get(0).getDuration() * tasks.get(0).getNeedCPU());
        int toAdd = 0;
        for (long adjId : adjIds) {
            toAdd = Math.max(toAdd, generateSchedulePriorityOfStage(stageMap.get(adjId), onlyAdjSchedulerPriority));
        }
        onlyAdjSchedulerPriority.put(stage.stageId, res);
        assert res + toAdd > 0;
        return res + toAdd;
    }

    @Override
    public List<List<TSDecision>> schedule(List<Stage> stages, Map<Long, List<Task>> stageIdToTasks) {
        return null;
    }

    @Override
    public double runTasks(List<List<TSDecision>> scheduleSlotList, boolean withCache, Map<Long, Task> taskMap) {
        return 0;
    }

    @Override
    public void createAndInitMaxHeap() {
        // create min heap and init it with adding some task
        taskMaxHeap = new PriorityQueue<>((o1, o2) -> o2.getSchedulePriority() - o1.getSchedulePriority());
        firstInitMaxHeap();
    }

    @Override
    public void initPriorityOfStages() {
        Map<Long, Integer> onlyAdjSP = new HashMap<>();
        for (Stage curStage : stageMap.values()) {
//        for (Map.Entry<Long, List<Task>> entry : stageIdToTasks.entrySet()) {
//            Stage curStage = stageMap.get(entry.getKey());
//            if (curStage == null) {
//                continue;
//            } // stage map是单个job的 //没必要遍历整个stageIdToTasks
            int otherSP = generateSchedulePriorityOfStage(curStage, onlyAdjSP);
            List<Task> tasks = stageIdToTasks.get(curStage.stageId);
            for (int i = 0; i < tasks.size(); i++) {
                Task curTask = tasks.get(i);
                curTask.setSchedulePriority(otherSP - i * curTask.getNeedCPU() * curTask.getDuration().intValue());
            }
        }
    }

}
