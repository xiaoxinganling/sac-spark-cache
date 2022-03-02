package task.scheduler;

import entity.Stage;
import entity.TSDecision;
import entity.Task;
import org.apache.log4j.Logger;
import task.TaskDispatcher;

import java.util.List;
import java.util.Map;

public class FIFOScheduler extends TaskScheduler {

    public Logger logger = Logger.getLogger(this.getClass());

    @Override
    public List<Map<Long, TSDecision>> schedule(List<Stage> stages, Map<Long, List<Task>> stageIdToTasks, TaskDispatcher td) {
        return null;
    }
}
