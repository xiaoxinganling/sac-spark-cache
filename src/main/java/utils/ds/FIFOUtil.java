package utils.ds;

import entity.RDD;

import java.util.*;

public class FIFOUtil extends ReplaceUtil {

    private Queue<RDD> containRDDs;

    public FIFOUtil() {
        containRDDs = new LinkedList<>();
        cachedRDDIds = new HashSet<>();
    }

    @Override
    public long addRDD(RDD rdd) {
        if(cachedRDDIds.contains(rdd.rddId)) {
            return 0;
        }
        containRDDs.offer(rdd);
        cachedRDDIds.add(rdd.rddId);
        return rdd.partitionNum;
    }

    @Override
    public RDD deleteRDD() {
        RDD rddToDelete = containRDDs.poll();
        if(rddToDelete != null) {
            cachedRDDIds.remove(rddToDelete.rddId);
        }
        return rddToDelete;
    }

    @Override
    public void clear() {
        containRDDs.clear();
        cachedRDDIds.clear();
    }

    @Override
    public Set<Long> getCachedRDDIds() {
        return cachedRDDIds;
    }

    @Override
    public RDD getRDD(long rddId) {
        return null; // KEYPOINT: no need to implement
    }

    @Override
    public Map getPriority() {
        Map<Long, Integer> map = new HashMap<>();
        LinkedList<RDD> linkedList = (LinkedList<RDD>) containRDDs;
        for (int i = 0; i < linkedList.size(); i++) {
            map.put(linkedList.get(i).rddId, i + 1); // 越小优先级越低
        }
        return map;
    }

}
