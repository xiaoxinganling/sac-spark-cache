package task;


import entity.Task;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class TaskRunner {

    public String taskRunnerId;

    public int coreNum;

    public int memorySize;

    private List<Queue<Task>> taskQueueList;

    private Logger logger = org.apache.log4j.Logger.getLogger(this.getClass());

    public TaskRunner(String taskRunnerId, int coreNum, int memorySize) {
        this.taskRunnerId = taskRunnerId;
        this.coreNum = coreNum;
        this.memorySize = memorySize;
        this.taskQueueList = new ArrayList<>();
        for (int i = 0; i < coreNum; i++) {
            taskQueueList.add(new LinkedList<>());
        }
        logger.info(String.format("TaskRunner [%s] created with [%d] CPU core and [%d] memory size",
                taskRunnerId, coreNum, memorySize));
    }

    public void receiveTask(Task task, int queueIndex) {
        taskQueueList.get(queueIndex).offer(task);
        logger.info(String.format("TaskRunner [%s] receive Task [%s] at Queue [%d]",
                taskRunnerId, task.getTaskId(), queueIndex));
    }

    public double runTasks() {
        double maxTime = 0;
        int queueIndex = 0;
        for (Queue<Task> taskQueue : taskQueueList) {
            if (taskQueue.isEmpty()) {
                continue;
            }
            double queueTime = 0;
            while (!taskQueue.isEmpty()) {
                queueTime += taskQueue.poll().getDuration();
            }
            logger.info(String.format("TaskRunner [%s] run tasks at Queue [%d] for [%f] ms.",
                    taskRunnerId, queueIndex++, queueTime));
            maxTime = Math.max(maxTime, queueTime);
        }
        return maxTime;
    }

    public int totalTaskSize() {
        int res = 0;
        for (Queue<Task> queue : taskQueueList) {
            res += queue.size();
        }
        return res;
    }

}
