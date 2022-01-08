package simulator;

import entity.RDD;
import entity.Stage;
import lombok.Data;
import org.apache.log4j.Logger;
import utils.CriticalPathUtil;

import java.io.IOException;
import java.util.*;

@Data
public class StageRunner {

    public String stageRunnerId;

    private Queue<Stage> stageQueue;

    private CacheSpace cacheSpace;

    private List<RDD> hotRDD;

    private Map<Long, RDD> hotRDDMap;

    public void setHotRDDMap(List<RDD> hotRDD) {
        hotRDDMap = new HashMap<>();
        for (RDD rdd : hotRDD) {
            hotRDDMap.put(rdd.rddId, rdd);
        }
    }

    private Set<Long> hotRDDIdSet;

    public void setHotRDD(List<RDD> hotRDD) {
        this.hotRDD = hotRDD;
    }


    public void setHotRDDIdSet(Set<Long> hotRDDIdSet) {
        this.hotRDDIdSet = hotRDDIdSet;
    }

    private Logger logger = Logger.getLogger(this.getClass());

    public StageRunner(String stageRunnerId) {
        logger.info(String.format("StageRunner [%s] is created.", stageRunnerId));
        this.stageRunnerId = stageRunnerId;
        stageQueue = new LinkedList<>();
    }

    public StageRunner(String stageRunnerId, CacheSpace cacheSpace) {
        logger.info(String.format("StageRunner [%s] is created with CacheSpace [%s].",
                stageRunnerId, cacheSpace.cacheSpaceId));
        this.stageRunnerId = stageRunnerId;
        stageQueue = new LinkedList<>();
        this.cacheSpace = cacheSpace;
    }

    public boolean getIsUsing() {
        return stageQueue.size() > 0;
    }

    public void receiveStage(Stage stage) {
        logger.info(String.format("StageRunner [%s] receives Stage [%d].", stageRunnerId, stage.stageId));
        stageQueue.offer(stage);
    }

    // return the runtime of stage queue
    public double runStages() {
        double res = 0;
        while(!stageQueue.isEmpty()) {
            Stage curStage = stageQueue.poll();
            logger.info(String.format("StageRunner [%s] is running Stage [%d].", stageRunnerId, curStage.stageId));
            double runTime = runTimeOfStage(curStage);
            res += runTime;
            logger.info(String.format("StageRunner [%s] has run Stage [%d] for [%f]s.", stageRunnerId, curStage.stageId, runTime));
        }
        return res;
    }

    private double runTimeOfStage(Stage stage) {
        return CriticalPathUtil.getLongestTimeOfStageWithSource(stage, null, CriticalPathUtil.STAGE_LAST_NODE, CriticalPathUtil.NO_NEED_FOR_PATH);
//        RDD lastRDD = SimpleUtil.lastRDDOfStage(stage);
//        Map<Long, RDD> rddMap = new HashMap<>();
//        for(RDD rdd : stage.rdds) {
//            rddMap.put(rdd.rddId, rdd);
//        }
//        return SimpleUtil.lastRDDTimeOfStage(rddMap, lastRDD);
    }

    public double runStagesWithCacheSpace(CacheSpace cacheSpace) {
        double res = 0;
        while(!stageQueue.isEmpty()) {
            // run every stage of StageRunner
            Stage curStage = stageQueue.poll();
            Set<Long> beforeSet = new HashSet<>(cacheSpace.getCachedRDDIds());
//            Map beforePriority = new HashMap(cacheSpace.getPriority());
            logger.info(String.format("StageRunner [%s] is running Stage [%d] with CacheSpace %s.",
                    stageRunnerId, curStage.stageId, beforeSet));
            if (cacheSpace.getPolicy() == ReplacePolicy.DP) {
                List<Long> computePath = new ArrayList<>();
                double runTime = CriticalPathUtil.getLongestTimeOfStageWithSource(curStage, cacheSpace, CriticalPathUtil.STAGE_LAST_NODE, computePath);
                StageDispatcher.updateCacheHitRatio(computePath, hotRDDIdSet, beforeSet);
                cacheSpace.changeAfterStageRun(curStage);
                for (long rddId : computePath) {
                    if (hotRDDIdSet.contains(rddId)) {
                        cacheSpace.addRDD(hotRDDMap.get(rddId));
                    }
                }
                res += runTime;
                logger.info(String.format("StageRunner [%s] has run Stage [%d] for [%f]s with visiting %s, CacheSpace %s -> %s.",
                        stageRunnerId, curStage.stageId, runTime, computePath, beforeSet, cacheSpace.getCachedRDDIds()));
                continue;
            }
//            double runTime = CriticalPathUtil.getLongestTimeOfStageWithSource(curStage, cacheSpace, CriticalPathUtil.STAGE_LAST_NODE, CriticalPathUtil.NO_NEED_FOR_PATH); //run之前更新
            List<Long> computePath = new ArrayList<>();
            double runTime = CriticalPathUtil.getLongestTimeOfStageWithSource(curStage, cacheSpace, CriticalPathUtil.STAGE_LAST_NODE, computePath);
            double contrastRunTime = CriticalPathUtil.getLongestTimeOfStageWithSource(curStage, null, CriticalPathUtil.STAGE_LAST_NODE, CriticalPathUtil.NO_NEED_FOR_PATH);// TODO: to delete for performance
            // update cache hit ratio
//            List<Long> computePath = new ArrayList<>();
//            for (RDD rdd : curStage.rdds) {
//                computePath.add(rdd.rddId);
//            }
            StageDispatcher.updateCacheHitRatio(computePath, hotRDDIdSet, beforeSet);
            // end update
            // after running stage, update MRDUtil's distance and LRCUtil's reference count
            cacheSpace.changeAfterStageRun(curStage);
            // end update
            // after running stage, add data into CacheSpace
//            if (cacheSpace.getPolicy() == ReplacePolicy.LRC || cacheSpace.getPolicy() == ReplacePolicy.MRD) {
//                curStage.rdds.sort((o1, o2) -> (int) (o1.rddId - o2.rddId)); // TODO: check sorting effects
//                for(RDD rdd : curStage.rdds) {
//                    // LRC是将所有的rdd都执行add操作
//                    if (hotRDDIdSet.contains(rdd.rddId) || cacheSpace.getPolicy() == ReplacePolicy.LRC) { // 不要重复添加 fix bug of repeatedly adding ` && !cacheSpace.getCachedRDDIds().contains(rdd.rddId)`
                        // for check
//                    beforeSet = new HashSet<>(cacheSpace.getCachedRDDIds());
//                    cacheSpace.addRDD(rdd); // 原本的代码
//                    try {
//                        if (hotRDDIdSet.contains(rdd.rddId)) {
//                            ValidationUI.writeLine(SimulatorProcess.curJobId, curStage.stageId, rdd,
//                                    beforeSet, cacheSpace.getCachedRDDIds(), beforePriority, cacheSpace.getPolicy(), cacheSpace.getPriority());
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
                        // end check
//                        cacheSpace.addRDD(rdd);
//                    }
//                }
//            }
//            if (cacheSpace.getPolicy() == ReplacePolicy.DP) {
//                for (long rddId : computePath) {
//                    if (hotRDDIdSet.contains(rddId)) {
//                        cacheSpace.addRDD(hotRDDMap.get(rddId));
//                    }
//                }
//            } else {
//                curStage.rdds.sort((o1, o2) -> (int) (o1.rddId - o2.rddId)); // TODO: check sorting effects
//                for(RDD rdd : curStage.rdds) {
//                    // LRC是将所有的rdd都执行add操作
//                    if (hotRDDIdSet.contains(rdd.rddId) || cacheSpace.getPolicy() == ReplacePolicy.LRC) { // 不要重复添加 fix bug of repeatedly adding ` && !cacheSpace.getCachedRDDIds().contains(rdd.rddId)`
                        // for check
//                    beforeSet = new HashSet<>(cacheSpace.getCachedRDDIds());
//                    cacheSpace.addRDD(rdd); // 原本的代码
//                    try {
//                        if (hotRDDIdSet.contains(rdd.rddId)) {
//                            ValidationUI.writeLine(SimulatorProcess.curJobId, curStage.stageId, rdd,
//                                    beforeSet, cacheSpace.getCachedRDDIds(), beforePriority, cacheSpace.getPolicy(), cacheSpace.getPriority());
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
                        // end check
//                        cacheSpace.addRDD(rdd);
//                    }
//                }
//            }
            // end add
            // add RDD to visit into CacheSpace
            for (long rddId : computePath) {
                if (hotRDDIdSet.contains(rddId)) {
                    cacheSpace.addRDD(hotRDDMap.get(rddId));
                }
            }
            // end RDD
            res += runTime;
//            logger.info(String.format("StageRunner [%s] has run Stage [%d] for [%f]s, contrast for [%f]s, CacheSpace %s -> %s.",
//                    stageRunnerId, curStage.stageId, runTime, contrastRunTime, beforeSet, cacheSpace.getCachedRDDIds()));
            logger.info(String.format("StageRunner [%s] has run Stage [%d] for [%f]s with visiting %s, contrast for [%f]s, CacheSpace %s -> %s.",
                    stageRunnerId, curStage.stageId, runTime, computePath, contrastRunTime, beforeSet, cacheSpace.getCachedRDDIds()));
        }
        return res;
    }


}
