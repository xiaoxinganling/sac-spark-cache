package simulator;

import entity.Job;
import entity.RDD;
import entity.Stage;

import java.util.*;

/**
 * 统计RDD的stage信息，得到rddId -> [stageIds]（顺序是stage出现的顺序）
 */
public class RDDStageInfoManager {

    public static final long MAX_DISTANCE = 999999;

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
