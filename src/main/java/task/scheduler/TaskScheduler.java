package task.scheduler;

import entity.Stage;
import entity.TSDecision;
import entity.Task;
import task.TaskDispatcher;
import java.util.List;
import java.util.Map;

public abstract class TaskScheduler {

    public abstract List<Map<Long, TSDecision>> schedule(List<Stage> stages, Map<Long, List<Task>> stageIdToTasks, TaskDispatcher td);

}
