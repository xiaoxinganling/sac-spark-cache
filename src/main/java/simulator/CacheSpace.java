package simulator;

import entity.RDD;
import org.apache.log4j.Logger;

import java.util.*;

public class CacheSpace {

    public String cacheSpaceId;

    private long totalSize;

    private long curSize;

    private ReplacePolicy policy;

    private Queue<RDD> containRDDs;

    public Set<Long> getCachedRDDIds() {
        return cachedRDDIds;
    }

    private Set<Long> cachedRDDIds;

    private Logger logger = Logger.getLogger(this.getClass());

    public CacheSpace(int totalSize, ReplacePolicy policy) {
        cacheSpaceId = String.valueOf(new Random().nextDouble());
        this.totalSize = totalSize;
        this.policy = policy;
        cachedRDDIds = new HashSet<>();
        containRDDs = new LinkedList<>();
        curSize = 0;
        logger.info(String.format("CacheSpace: initialize with size of [%d] and evicting policy [%s].",
                totalSize, policy));
    }

    public boolean rddInCacheSpace(long rddId) {
        boolean cacheHit = cachedRDDIds.contains(rddId);
        if(cacheHit) {
            logger.info(String.format("CacheSpace: cache hit RDD [%d].", rddId));
        }else{
//            logger.info(String.format("CacheSpace: cache miss RDD [%d].", rddId));
        }
        return cacheHit;
    }

    public void addRDD(RDD rdd) {
        // TODO: implement them
        if(policy == ReplacePolicy.LRU) {

        }else if(policy == ReplacePolicy.LFU) {

        }else if(policy == ReplacePolicy.LRC) {

        }else if(policy == ReplacePolicy.MRD) {

        }else if(policy == ReplacePolicy.DP) {

        }else{
            while(cachedRDDIds.size() > 0 && totalSize - curSize < rdd.partitionNum) {
                RDD rddToDelete = containRDDs.poll(); // fix: [2, 3] 10不删2，而是3，这不是FIFO
                containRDDs.remove(rddToDelete);
                cachedRDDIds.remove(rddToDelete.rddId);
                curSize -= rddToDelete.partitionNum;
                logger.info(String.format("CacheSpace: delete RDD [%d] with size [%d] by policy [%s], current size [%d / %d].",
                        rddToDelete.rddId, rdd.partitionNum, policy, curSize, totalSize));
            }
            if(totalSize - curSize < rdd.partitionNum) {
                logger.info(String.format("CacheSpace: ignore RDD [%d] with size [%d], current size [%d / %d].",
                        rdd.rddId, rdd.partitionNum, curSize, totalSize));
            }else{
                containRDDs.offer(rdd);
                cachedRDDIds.add(rdd.rddId);
                curSize += rdd.partitionNum;
                logger.info(String.format("CacheSpace: add RDD [%d] with size [%d], current size [%d / %d].",
                        rdd.rddId, rdd.partitionNum, curSize, totalSize));
            }
        }
//        containRDDs.add(rdd);
//        rddIds.add(rdd.rddId);
//        totalSize -= rdd.memorySize;
    }

    public void clear(String newApplication) {
        curSize = 0;
        containRDDs.clear();
        cachedRDDIds.clear();
        logger.info(String.format("CacheSpace: clear RDDs because of [%s], current size [%d / %d].",
                newApplication, curSize, totalSize));
    }

}
