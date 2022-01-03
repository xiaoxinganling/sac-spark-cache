package utils.ds;

import entity.RDD;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

public class FIFOUtil extends ReplaceUtil {

    private Queue<RDD> containRDDs;

    public FIFOUtil() {
        containRDDs = new LinkedList<>();
        cachedRDDIds = new HashSet<>();
    }

    @Override
    public long addRDD(RDD rdd) {
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

}
