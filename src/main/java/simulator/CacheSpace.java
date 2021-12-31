package simulator;

import entity.RDD;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class CacheSpace {

    private int totalSize;

    private int curSize;

    private ReplacePolicy policy;

    private Set<RDD> containRDDs;

    public Set<Long> getRddIds() {
        return rddIds;
    }

    private Set<Long> rddIds;

    private Logger logger = Logger.getLogger(this.getClass());

    public CacheSpace(int totalSize, ReplacePolicy policy) {
        this.totalSize = totalSize;
        this.policy = policy;
        rddIds = new HashSet<>();
        containRDDs = new HashSet<>();
        curSize = 0;
    }

    public boolean rddInCacheSpace(long rddId) {
        return rddIds.contains(rddId);
    }

    public void addRDD(RDD rdd) {
//        if(totalSize - curSize >= rdd.memorySize) {
//            containRDDs.add(rdd);
//            rddIds.add(rdd.rddId);
//            totalSize -= rdd.memorySize;
//            return;
//        }
//        // TODO: implement them
//        if(policy == ReplacePolicy.LRU) {
//
//        }else if(policy == ReplacePolicy.LFU) {
//
//        }else if(policy == ReplacePolicy.LRC) {
//
//        }else if(policy == ReplacePolicy.MRD) {
//
//        }else if(policy == ReplacePolicy.DP) {
//
//        }else{
//            RDD rddToDelete = containRDDs.iterator().next();
//            containRDDs.remove(rddToDelete);
//            rddIds.remove(rddToDelete.rddId);
//        }
        containRDDs.add(rdd);
        rddIds.add(rdd.rddId);
        totalSize -= rdd.memorySize;
    }

}
