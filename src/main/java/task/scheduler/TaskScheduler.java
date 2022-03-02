package task.scheduler;

import entity.Stage;
import entity.TSDecision;
import entity.Task;
import task.TaskDispatcher;
import java.util.List;
import java.util.Map;

public abstract class TaskScheduler {

    // 用于调度task、调用TaskRunner执行task
    public TaskDispatcher td;

    public TaskScheduler(TaskDispatcher td) {
        this.td = td;
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
    public abstract double runTask(List<List<TSDecision>> scheduleSlotList, boolean withCache, Map<Long, Task> taskMap);

}
