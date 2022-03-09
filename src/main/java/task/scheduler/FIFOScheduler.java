package task.scheduler;

import entity.Stage;
import entity.TSDecision;
import entity.Task;
import task.TaskDispatcher;

import java.util.*;

public class FIFOScheduler extends TaskScheduler {

    public FIFOScheduler() {
    }

    public FIFOScheduler(TaskDispatcher td) {
        super(td);
    }

    public FIFOScheduler(TaskDispatcher td, Map<Long, List<Task>> stageIdToTasks) {
        super(td, stageIdToTasks);
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
        taskMaxHeap = new PriorityQueue<>((o1, o2) -> (int) (o1.getTaskId() - o2.getTaskId()));
        firstInitMaxHeap();
    }

    @Override
    public void initPriorityOfStages() {
        // do nothing
    }

}
