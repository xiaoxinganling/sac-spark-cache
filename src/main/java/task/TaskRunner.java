package task;


import entity.Partition;
import entity.RDD;
import entity.Task;
import org.apache.log4j.Logger;
import utils.CriticalPathUtil;
import java.util.*;

public class TaskRunner {

    public static final double NO_TASK_TIME = -1;

    public String taskRunnerId;

    public int coreNum;

    public int memorySize;

    private List<Queue<Task>> taskQueueList;

    public List<Queue<Task>> getTaskQueueList() {
        return taskQueueList;
    }

    public void setHotRDDIdSet(Set<Long> hotRDDIdSet) {
        this.hotRDDIdSet = hotRDDIdSet;
    }

    public void setHotPartitionMap(Map<String, Partition> hotPartitionMap) {
        this.hotPartitionMap = hotPartitionMap;
    }

    private Set<Long> hotRDDIdSet; // TODO: maintain

    private Map<String, Partition> hotPartitionMap; // TODO: maintain

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
//        logger.info(String.format("TaskRunner [%s] receive Task [%s] at Queue [%d]",
//                taskRunnerId, task.getTaskId(), queueIndex));
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
//            logger.info(String.format("TaskRunner [%s] run tasks at Queue [%d] for [%f] ms.",
//                    taskRunnerId, queueIndex++, queueTime));
            maxTime = Math.max(maxTime, queueTime);
        }
        return maxTime;
    }

    public double runOneTaskWithSCacheSpace(int queueIndex, SCacheSpace sCacheSpace) {
        if (queueIndex >= taskQueueList.size() || taskQueueList.get(queueIndex).isEmpty()) {
            return NO_TASK_TIME;
        }
        Task task = taskQueueList.get(queueIndex).poll();
        assert task != null;
        List<String> computePath = new ArrayList<>();
        double time = CriticalPathUtil.getLongestTimeOfTaskWithSource(task, sCacheSpace, CriticalPathUtil.TASK_LAST_NODE, computePath);
        // TODO: update cache hit ratio
        sCacheSpace.changeAfterTaskRun(task);
        for (String partitionId : computePath) {
            if (hotRDDIdSet.contains(Long.parseLong(partitionId.split(CriticalPathUtil.PARTITION_FLAG)[0]))) {
                sCacheSpace.addPartition(hotPartitionMap.get(partitionId));
            }
        }
        return time;
    }

    public int totalTaskSize() {
        int res = 0;
        for (Queue<Task> queue : taskQueueList) {
            res += queue.size();
        }
        return res;
    }

    public Task getOneTaskWithOnlyOneQueue() {
        return taskQueueList.get(0).peek();
    }

    public Task pollOneTaskWithOnlyOneQueue() {
        return taskQueueList.get(0).poll();
    }

}
