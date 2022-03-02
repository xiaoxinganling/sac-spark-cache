package task.scheduler;

import entity.Stage;
import entity.TSDecision;
import entity.Task;
import org.apache.log4j.Logger;
import task.TaskDispatcher;

import java.util.*;

public class FIFOScheduler extends TaskScheduler {

    public Logger logger = Logger.getLogger(this.getClass());

    public FIFOScheduler(TaskDispatcher td) {
        super(td);
        logger.info(String.format("Scheduler [FIFO] initializes with TaskDispatcher [%s]", td.taskDispatcherId));
    }

    @Override
    public List<List<TSDecision>> schedule(List<Stage> stages, Map<Long, List<Task>> stageIdToTasks) {
        // res to return
        List<List<TSDecision>> res = new ArrayList<>();
        // FIFO
        stages.sort((o1, o2) -> (int) (o1.stageId - o2.stageId));
        // record each batch of stages
        List<Stage> stageBatch = new ArrayList<>();
        Set<Long> finishedStageIds = new HashSet<>();
        for (Stage stage : stages) {
            boolean requireContinue = true;
            for (long parentId : stage.parentIDs) {
                if (!finishedStageIds.contains(parentId)) {
                    requireContinue = false;
                    break;
                }
            }
            if (!requireContinue) {
                List<TSDecision> tsDecisions = td.dispatchTask(stageIdToTasks, stageBatch);
                logger.info(String.format("Scheduler [FIFO] schedule tasks as %s.", tsDecisions));
                res.add(tsDecisions);
                for (Stage finished : stageBatch) {
                    finishedStageIds.add(finished.stageId);
                }
                stageBatch = new ArrayList<>();
            }
            stageBatch.add(stage);
        }
        List<TSDecision> tsDecisions = td.dispatchTask(stageIdToTasks, stageBatch);
        logger.info(String.format("Scheduler [FIFO] schedule tasks as %s.", tsDecisions));
        res.add(tsDecisions);
        return res;
    }

    @Override
    public double runTask(List<List<TSDecision>> scheduleSlotList, boolean withCache, Map<Long, Task> taskMap) {
        if (!withCache) {
            double res = 0;
            for (List<TSDecision> parallelSlot : scheduleSlotList) {
                double curTime = td.runTasks(parallelSlot, taskMap);
                res += curTime;
                logger.info(String.format("Scheduler [FIFO] call TaskDispatcher [%s] to run tasks described as %s for [%f] ms.",
                        td.taskDispatcherId, parallelSlot, curTime));
            }
            return res;
        }
        return 0;
    }

}
