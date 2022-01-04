package utils.ds;

import entity.RDD;
import simulator.ReferenceCountManager;

import java.util.*;

/**
 * my own implementation of `LRC`: least reference count
 */
public class LRCUtil extends ReplaceUtil {

    private Map<Long, Integer> hotDataRC;

    private Map<Long, Integer> rddIdToActionNum;

    // 略显丑陋的实现
    private LinkedList<RDD> containsRDDs;

    public void setHotDataRC(Map<Long, Integer> hotDataRC) {
        this.hotDataRC = hotDataRC;
    }

    public void setRddIdToActionNum(Map<Long, Integer> rddIdToActionNum) {
        this.rddIdToActionNum = rddIdToActionNum;
    }

    public void updateHotDataRefCountByRDD(RDD rdd) {
        ReferenceCountManager.updateHotDataRefCountByRDD(hotDataRC, rdd, rddIdToActionNum);
    }

    // 每次新application，都要初始化hotDataRC和rddIdToActionNum
    public LRCUtil() {
        // fix: 所有的DS必须初始化
        hotDataRC = new HashMap<>();
        rddIdToActionNum = new HashMap<>();
        containsRDDs = new LinkedList<>();
        cachedRDDIds = new HashSet<>();
    }

    @Override
    public long addRDD(RDD rdd) {
        containsRDDs.add(rdd);
        cachedRDDIds.add(rdd.rddId);
        return rdd.partitionNum;
    }

    @Override
    public RDD deleteRDD() {
        // 假设delete一系列RDD时已排好序
        RDD toDelete = containsRDDs.removeFirst();
        if(toDelete != null) {
            cachedRDDIds.remove(toDelete.rddId);
        }
        return toDelete;
    }

    public void sortRDDByRC() {
        containsRDDs.sort(Comparator.comparingInt(o -> hotDataRC.get(o.rddId)));
    }

    @Override
    public void clear() {
        hotDataRC.clear();
        rddIdToActionNum.clear();
        containsRDDs.clear();
        cachedRDDIds.clear();
    }

    @Override
    public Set<Long> getCachedRDDIds() {
        return cachedRDDIds;
    }

    @Override
    public RDD getRDD(long rddId) {
        return null; // LRC不必实现
    }
}
