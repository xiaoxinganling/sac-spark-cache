package simulator;

import entity.Job;
import entity.RDD;
import entity.Stage;
import org.apache.log4j.Logger;
import simulator.dp.KeyPathManager;
import simulator.dp.RDDTimeManager;
import utils.ds.*;

import java.util.*;

public class CacheSpace {

    public String cacheSpaceId;

    private long totalSize;

    private long curSize;

    private ReplacePolicy policy;

    public ReplacePolicy getPolicy() {
        return policy;
    }

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
                replaceUtil = new DPUtil();
                break;
            default:
        }
        curSize = 0;
        logger.info(String.format("CacheSpace: initialize with size of [%d] and evicting policy [%s].",
                totalSize, policy));
    }

    public void getRDD(long rddId) {
        replaceUtil.getRDD(rddId);
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
                // 首先更新hotDataRC，FIXME: 这里不更新了
                LRCUtil lrcUtil = (LRCUtil) replaceUtil;
//                lrcUtil.updateHotDataRefCountByRDD(rdd);
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
            {
                DPUtil dpUtil = (DPUtil) replaceUtil;
                // 1. 无需添加
                // 2. 需添加，总大小不够
                if (getCachedRDDIds().contains(rdd.rddId) || totalSize < rdd.partitionNum) {
                    return false;
                }
                // 3. 可直接添加
                if (totalSize - curSize >= rdd.partitionNum) {
                    dpUtil.addRDD(rdd);
                    curSize += rdd.partitionNum; // fix: 忘记更新curSize了, 写成了curSize+=totalSize
                    return true;
                }
                // 4. 需添加，需决策
                Set<Long> rddToAdd = dpUtil.doDp(rdd, totalSize);
                Set<Long> cachedRDDIds = new HashSet<>(getCachedRDDIds());
                for (long curCachedRDDId : cachedRDDIds) {
                    if (!rddToAdd.contains(curCachedRDDId)) {
                        RDD toDelete = dpUtil.deleteRDD(curCachedRDDId);
                        curSize -= toDelete.partitionNum; // fix：更新curSize -> 不需要curSize
                    }
                }
                if (rddToAdd.contains(rdd.rddId)) {
                    dpUtil.addRDD(rdd);
                    curSize += rdd.partitionNum; // fix: 更新curSize
                }
                return true;
            }
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
        } else if (policy == ReplacePolicy.DP) {
            RDDTimeManager.timeMemMap = new HashMap<>();
            DPUtil dpUtil = (DPUtil) replaceUtil;
            dpUtil.setKeyStages(KeyPathManager.generateKeyStages(jobList));
        }
        logger.info(String.format("CacheSpace: prepare for new application [%s].", newApplication));
    }

    public void changeAfterStageRun(Stage stage) {
        if (policy == ReplacePolicy.MRD) {
            MRDUtil mrdUtil = (MRDUtil) replaceUtil;
            mrdUtil.updateRDDDistanceByStage(stage);
            logger.info(String.format("CacheSpace: update RDD Distance of policy [%s] after running Stage [%d].",
                    policy, stage.stageId));
        } else if (policy == ReplacePolicy.LRC) {
            LRCUtil lrcUtil = (LRCUtil) replaceUtil;
            lrcUtil.updateHotDataRefCountByStage(stage);
            logger.info(String.format("CacheSpace: update RDD Reference Count of policy [%s] after running Stage [%d].",
                    policy, stage.stageId));
        } else if (policy == ReplacePolicy.DP) {
            DPUtil dpUtil = (DPUtil) replaceUtil;
            dpUtil.updateKeyStagesByStage(stage);
            logger.info(String.format("CacheSpace: update key stages of policy [%s] after running Stage [%d].",
                    policy, stage.stageId));
        }
    }

    public Map getPriority() {
        return replaceUtil.getPriority();
    }

}
