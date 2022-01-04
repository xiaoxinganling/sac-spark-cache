package simulator;

import entity.Job;
import entity.RDD;
import entity.Stage;
import org.apache.log4j.Logger;
import utils.ds.*;

import java.util.*;

public class CacheSpace {

    public String cacheSpaceId;

    private long totalSize;

    private long curSize;

    private ReplacePolicy policy;

    private ReplaceUtil replaceUtil; //TODO: to combine all replace algorithms

    private Logger logger = Logger.getLogger(this.getClass());

    public Set<Long> getCachedRDDIds() {
        return replaceUtil.getCachedRDDIds();
    }

    public CacheSpace(int totalSize, ReplacePolicy policy) { // TODO: here int, but long declared
        cacheSpaceId = String.valueOf(new Random().nextDouble());
        this.totalSize = totalSize;
        this.policy = policy;
        switch (policy) {
            case FIFO:
                replaceUtil = new FIFOUtil();
                break;
            case LRU:
                replaceUtil = new LRUUtil();
                break;
            case LFU:
                replaceUtil = new LFUUtil();
                break;
            case LRC:
                replaceUtil = new LRCUtil();
                break;
            case MRD:
                replaceUtil = new MRDUtil();
                break;
            case DP:
                break;
            default:
        }
        curSize = 0;
        logger.info(String.format("CacheSpace: initialize with size of [%d] and evicting policy [%s].",
                totalSize, policy));
    }

    public boolean rddInCacheSpace(long rddId) {
        boolean cacheHit = replaceUtil.getCachedRDDIds().contains(rddId);
        replaceUtil.getRDD(rddId); // TODO: do something for this adding RDD, maybe log
        if(cacheHit) {
//            logger.info(String.format("CacheSpace: cache hit RDD [%d], policy [%s].", rddId, policy));
        }else{
//            logger.info(String.format("CacheSpace: cache miss RDD [%d].", rddId));
        }
        return cacheHit;
    }

    /**
     * add RDD with deleting when Cache Space is full
     * @param rdd
     * @return whether the adding is successful
     */
    public boolean addRDD(RDD rdd) {
        // TODO: implement them
        switch (policy) {
            case FIFO:
            case LRU:
            case LFU:
            {
                if(rdd.partitionNum > totalSize) {
//                    logger.info(String.format("CacheSpace: ignore RDD [%d] with size [%d], current size [%d / %d].",
//                            rdd.rddId, rdd.partitionNum, curSize, totalSize));
                    return false;
                }// fix: remove unnecessary delete
                while(!getCachedRDDIds().contains(rdd.rddId) && totalSize - curSize < rdd.partitionNum) { // fix: `replaceUtil.getCachedRDDIds().size() > 0 && ` is not required
//                    RDD rddToDelete = containRDDs.poll(); // fix: [2, 3] 10不删2，而是3，这不是FIFO
//                containRDDs.remove(rddToDelete); // fix: 这里已经删除了
                    RDD rddToDelete = replaceUtil.deleteRDD(); // TODO check null
                    curSize -= rddToDelete.partitionNum;
//                    logger.info(String.format("CacheSpace: delete RDD [%d] with size [%d] by policy [%s], current size [%d / %d].",
//                            rddToDelete.rddId, rddToDelete.partitionNum, policy, curSize, totalSize)); // fix: rdd.partitionNum -> rddToDelete.partitionNum
                }
                long changedSize = replaceUtil.addRDD(rdd);
                curSize += changedSize;
//                logger.info(String.format("CacheSpace: add RDD [%d] with size [%d], current size [%d / %d].",
//                        rdd.rddId, rdd.partitionNum, curSize, totalSize));
                return changedSize > 0;
            }
            case LRC:
            {
                // 首先更新hotDataRC
                LRCUtil lrcUtil = (LRCUtil) replaceUtil;
                lrcUtil.updateHotDataRefCountByRDD(rdd);
                // 1. 无需添加
                // 2. 需添加，总大小不够
                if (getCachedRDDIds().contains(rdd.rddId) || totalSize < rdd.partitionNum) {
                    return false;
                }
                // 3. 需添加，需决策
                curSize += lrcUtil.addRDD(rdd);
                lrcUtil.sortRDDByRC();
                while (curSize > totalSize) {
                    curSize -= lrcUtil.deleteRDD().partitionNum; // 可保证`lrcUtil.deleteRDD()`!=null
                }
                return true;
            }
            case MRD:
            {
                MRDUtil mrdUtil = (MRDUtil) replaceUtil;
                // 1. 无需添加
                // 2. 需添加，总大小不够
                if (getCachedRDDIds().contains(rdd.rddId) || totalSize < rdd.partitionNum) {
                    return false;
                }
                // 3. 需添加，需决策
                curSize += mrdUtil.addRDD(rdd);
                mrdUtil.sortRDDByDistance();
                while (curSize > totalSize) {
                    curSize -= mrdUtil.deleteRDD().partitionNum; // 可保证`mrdUtil.deleteRDD()`!=null
                }
                return true;
            }
            case DP:
                break;
            default:
        }
        return true; // just for return
    }

    public void prepare(String newApplication, List<Job> jobList, List<RDD> hotData) {
        curSize = 0;
        replaceUtil.clear();
        if (policy == ReplacePolicy.LRC) {
            LRCUtil lrcUtil = (LRCUtil) replaceUtil;
            lrcUtil.setHotDataRC(ReferenceCountManager.generateRefCountForHotData(jobList, hotData));
            lrcUtil.setRddIdToActionNum(ReferenceCountManager.generateRDDIdToActionNum(jobList));
        } else if (policy == ReplacePolicy.MRD) {
            MRDUtil mrdUtil = (MRDUtil) replaceUtil;
            mrdUtil.setRddToStageIds(RDDStageInfoManager.generateDistanceForHotData(jobList, hotData));
        }
        logger.info(String.format("CacheSpace: prepare for new application [%s].", newApplication));
    }

    public void changeAfterStageRun(Stage stage) {
        if (policy == ReplacePolicy.MRD) {
            MRDUtil mrdUtil = (MRDUtil) replaceUtil;
            mrdUtil.updateRDDDistanceByStage(stage);
            logger.info(String.format("CacheSpace: update RDD Distance of policy [%s] after running Stage [%d].",
                    policy, stage.stageId));
        }
    }

}
