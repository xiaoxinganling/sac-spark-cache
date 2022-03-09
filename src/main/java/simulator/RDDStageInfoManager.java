package simulator;

import entity.*;

import java.util.*;

/**
 * 统计RDD的stage信息，得到rddId -> [stageIds]（顺序是stage出现的顺序）
 */
public class RDDStageInfoManager {

    public static final long MAX_DISTANCE = 999999;

    public static final long MAX_TASK_DISTANCE = Long.MAX_VALUE - 5;

    // 可用最小堆orLinkedList实现, TODO: stage or stageId? stageId√
    public static Map<Long, PriorityQueue<Long>> generateDistanceForHotData(List<Job> jobList, List<RDD> hotData) {
        Map<Long, PriorityQueue<Long>> rddToStageIds = new HashMap<>();
        Set<Long> hotDataIds = new HashSet<>();
        for (RDD rdd : hotData) {
            hotDataIds.add(rdd.rddId);
        }
        for (Job job : jobList) {
            for (Stage stage : job.stages) {
                for (RDD rdd : stage.rdds) {
                    long key = rdd.rddId;
                    if (hotDataIds.contains(key)) {
                        // FIXME: 假定stage id小的先出现
                        // 为什么没有排序？ => 因为只保证了堆顶最小呀
                        PriorityQueue<Long> minHeap = rddToStageIds.getOrDefault(key, new PriorityQueue<>((o1, o2) -> (int) (o1 - o2)));
                        minHeap.offer(stage.stageId);
                        rddToStageIds.put(key, minHeap);
                    }
                }
            }
        }
        return rddToStageIds;
    }

    public static Map<String, PriorityQueue<Long>> generateDistanceForHotPartitions(Map<Long, List<Task>> stageIdToTasks,
                                                                                    List<Partition> hotPartitions) {
        Map<String, PriorityQueue<Long>> partitionToTaskIds = new HashMap<>();
        Set<String> hotPartitionIds = new HashSet<>();
        for (Partition p : hotPartitions) {
            hotPartitionIds.add(p.getPartitionId());
        }
        for (List<Task> tasks : stageIdToTasks.values()) {
            for (Task t : tasks) {
                for (Partition partition : t.getPartitions()) {
                    String key = partition.getPartitionId();
                    if (hotPartitionIds.contains(key)) {
                        PriorityQueue<Long> minHeap = partitionToTaskIds.getOrDefault(key, new PriorityQueue<>((o1, o2) -> (int) (o1 - o2)));
                        minHeap.offer(t.getTaskId());
                        partitionToTaskIds.put(key, minHeap);
                    }
                }
            }
        }
        return partitionToTaskIds;

    }

    public static void updateDistance(Map<Long, PriorityQueue<Long>> rddToStageIds, Stage stage) {
        for (RDD rdd : stage.rdds) {
            long key = rdd.rddId;
            if (rddToStageIds.containsKey(key)) {
                PriorityQueue<Long> stageIds = rddToStageIds.get(key);
                if (stage.stageId.equals(stageIds.peek())) {
                    stageIds.poll();
                    if (stageIds.size() == 0) {
                        stageIds.offer(MAX_DISTANCE);
                    }
                } // => 兼容所有情况
//                assert stage.stageId.equals(stageIds.peek());
//                stageIds.poll(); // check no need to rewrite? -> no need
            }
        }
    }


    public static void updatePartitionDistance(Map<String, PriorityQueue<Long>> partitionToTaskIds, Task task) {
        for (Partition partition : task.getPartitions()) {
            String key = partition.getPartitionId();
            if (partitionToTaskIds.containsKey(key)) {
                PriorityQueue<Long> taskIds = partitionToTaskIds.get(key);
//                if (task.getTaskId().equals(taskIds.peek())) {
//                    taskIds.poll();
//                    if (taskIds.size() == 0) {
//                        taskIds.offer(MAX_TASK_DISTANCE);
//                    }
//                }
                taskIds.remove(task.getTaskId());
                if (taskIds.size() == 0) {
                    taskIds.offer(MAX_TASK_DISTANCE);
                }
            }
        }
    }

    public static Map<Long, List<Long>> generateStageToRDDIds(Map<Long, PriorityQueue<Long>> rddToStageIds) {
        Map<Long, List<Long>> stageToRDDs = new HashMap<>();
        for (Map.Entry<Long, PriorityQueue<Long>> entry : rddToStageIds.entrySet()) {
            long key = entry.getKey();
            PriorityQueue<Long> value = entry.getValue();
            while (!value.isEmpty()) {
                long curKey = value.poll();
                List<Long> tmp = stageToRDDs.getOrDefault(curKey, new ArrayList<>());
                tmp.add(key);
                stageToRDDs.put(curKey, tmp);
            }
        }
        return stageToRDDs;
    }

}
