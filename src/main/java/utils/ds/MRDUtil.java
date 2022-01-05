package utils.ds;

import entity.RDD;
import entity.Stage;
import simulator.RDDStageInfoManager;
import java.util.*;

/**
 * algorithm of the most reference distance
 */
public class MRDUtil extends ReplaceUtil{

    private Map<Long, PriorityQueue<Long>> rddToStageIds;

    private LinkedList<RDD> containsRDDs;

    public void setRddToStageIds(Map<Long, PriorityQueue<Long>> rddToStageIds) {
        this.rddToStageIds = rddToStageIds;
    }

    public MRDUtil() {
        containsRDDs = new LinkedList<>(); // need to delete?
        rddToStageIds = new HashMap<>();
        cachedRDDIds = new HashSet<>();
    }

    public void updateRDDDistanceByStage(Stage stage) { // TODO: stage or stage list?
        RDDStageInfoManager.updateDistance(rddToStageIds, stage);
    }

    public void sortRDDByDistance() {
        // 保证minHeap里面永远有内容
//        containsRDDs.sort((o1, o2) -> (int) (rddToStageIds.get(o2.rddId).peek() - rddToStageIds.get(o1.rddId).peek()));
        containsRDDs.sort((o1, o2) -> {
            int distanceDiff = (int) (rddToStageIds.get(o2.rddId).peek() - rddToStageIds.get(o1.rddId).peek());
            return distanceDiff == 0 ? (int) (o2.rddId - o1.rddId) : distanceDiff;
        });
    }

    @Override
    public long addRDD(RDD rdd) {
        containsRDDs.add(rdd);
        cachedRDDIds.add(rdd.rddId);
        return rdd.partitionNum;
    }

    @Override
    public RDD deleteRDD() {
        // 假设delete前已排好序，这里的实现比较丑陋，暂时不考虑效率更高的实现
        RDD toDelete = containsRDDs.removeFirst();
        if(toDelete != null) {
            cachedRDDIds.remove(toDelete.rddId);
        }
        return toDelete;
    }

    @Override
    public void clear() {
        rddToStageIds.clear();
        containsRDDs.clear();
        cachedRDDIds.clear();
    }

    @Override
    public Set<Long> getCachedRDDIds() {
        return cachedRDDIds;
    }

    @Override
    public RDD getRDD(long rddId) {
        return null; // MRD不必实现，仅限于LRU和LFU
    }

    @Override
    public Map getPriority() {
        Map<Long, Long> tmpMap = new HashMap<>();
        for (Map.Entry<Long, PriorityQueue<Long>> entry : rddToStageIds.entrySet()) {
            tmpMap.put(entry.getKey(), entry.getValue().peek());
        }
//        return rddToStageIds; // TODO: 考虑只取top？
        return tmpMap;
    }
}
