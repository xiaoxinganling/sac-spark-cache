package task.scheduler;

import entity.Stage;
import entity.TSDecision;
import entity.Task;
import org.apache.log4j.Logger;
import task.TaskDispatcher;
import java.util.*;

/**
 * FIFO Scheduler with each task consuming one CPU resource
 */
public class SimpleFIFOScheduler extends TaskScheduler {

    public Logger logger = Logger.getLogger(this.getClass());

    public SimpleFIFOScheduler(TaskDispatcher td) {
        super(td);
        logger.info(String.format("Scheduler [SimpleFIFO] initializes with TaskDispatcher [%s]", td.taskDispatcherId));
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
        Set<Long> totalStageIds = new HashSet<>();
        for (Stage s : stages) {
            totalStageIds.add(s.stageId);
        }
        for (Stage stage : stages) {
            boolean requireContinue = true;
            // KEYPOINT: 如果是按照job依次schedule，需要过滤下parent (不过滤的话会提交一次空 stage list，非常巧合)
            for (long parentId : stage.parentIDs) {
                if (totalStageIds.contains(parentId) && !finishedStageIds.contains(parentId)) {
//                if (!finishedStageIds.contains(parentId)) {
                    requireContinue = false;
                    break;
                }
            }
            if (!requireContinue) {
                List<TSDecision> tsDecisions = td.dispatchTask(stageIdToTasks, stageBatch);
                logger.info(String.format("Scheduler [FIFO] schedule [%d] Stages, start with [%d]'s [%d] tasks as %s.",
                        stageBatch.size(), stageBatch.get(0).stageId, tsDecisions.size(), tsDecisions));
                res.add(tsDecisions);
                for (Stage finished : stageBatch) {
                    finishedStageIds.add(finished.stageId);
                }
                stageBatch = new ArrayList<>();
            }
            stageBatch.add(stage);
        }
        List<TSDecision> tsDecisions = td.dispatchTask(stageIdToTasks, stageBatch);
        logger.info(String.format("Scheduler [FIFO] schedule [%d] Stages, start with [%d]'s [%d] tasks as %s.",
                stageBatch.size(), stageBatch.get(0).stageId, tsDecisions.size(), tsDecisions));
        res.add(tsDecisions);
        return res;
    }

    @Override
    public double runTasks(List<List<TSDecision>> scheduleSlotList, boolean withCache, Map<Long, Task> taskMap) {
        double res = 0;
        for (List<TSDecision> parallelSlot : scheduleSlotList) {
            double curTime = td.runTasks(parallelSlot, taskMap, withCache);
            res += curTime;
            logger.info(String.format("Scheduler [FIFO] call TaskDispatcher [Stage %s] to run tasks for [%f] ms described as %s.",
                    td.taskDispatcherId, curTime, parallelSlot));
        }
        return res;
    }

    @Override
    public void createAndInitMaxHeap() {

    }

    @Override
    public void initPriorityOfStages() {

    }

    @Override
    public List<Task> provideTasksUnderResourceLimit(List<Task> finishedTask, int idleCPU) {
        return null;
    }

}
