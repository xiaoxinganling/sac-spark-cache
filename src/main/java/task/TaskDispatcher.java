package task;

import entity.Job;
import entity.Stage;
import entity.TSDecision;
import entity.Task;
import org.apache.log4j.Logger;
import simulator.JobStageSubmitter;
import simulator.TaskGenerator;

import java.io.IOException;
import java.util.*;

public class TaskDispatcher {

    public String taskDispatcherId;

    private TaskRunner[] taskRunners;

    private Logger logger = Logger.getLogger(this.getClass());

    public TaskDispatcher(String taskDispatcherId, int taskRunnerSize) {
        logger.info(String.format("TaskDispatcher [%s] is created with [%d] TaskRunner.",
                taskDispatcherId, taskRunnerSize));
        this.taskDispatcherId = taskDispatcherId;
        taskRunners = new TaskRunner[taskRunnerSize];
        for (int i = 0; i < taskRunnerSize; i++) {
            taskRunners[i] = new TaskRunner(String.format("%s-T-R-%d", taskDispatcherId, i), 1, 1);
        }
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
            double curTime = runTasks(tsDecisions, taskMap);
            jobTotalTime += curTime;
//            sb.append(tmp.get(0).stageId).append(":").append(curTime).append("\n");
            logger.info(String.format("TaskDispatcher [%s] dispatch tasks in [%d] Stages, first Stage id: [%d], and run for [%f] ms.",
                    taskDispatcherId, tmp.size(), tmp.get(0).stageId, curTime));
            List<Stage> toSubmit;
            while ((toSubmit = jss.submitAvailableStages()) != null) {
                tsDecisions = dispatchTask(stageIdToTasks, toSubmit);
                curTime = runTasks(tsDecisions, taskMap);
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

    public double runTasks(List<TSDecision> taskScheduleDecision, Map<Long, Task> taskMap) {
        for (TSDecision tsDecision : taskScheduleDecision) {
            taskRunners[tsDecision.taskRunnerIndex].receiveTask(taskMap.get(tsDecision.taskId),
                    tsDecision.queueIndex);
        }
        logger.info(String.format("TaskDispatcher [%s] is instructing TaskRunner to run Tasks.", taskDispatcherId));
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

}
