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
        return CriticalPathUtil.getLongestTimeOfStage(stage, null);
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
            Map beforePriority = new HashMap(cacheSpace.getPriority());
            logger.info(String.format("StageRunner [%s] is running Stage [%d] with CacheSpace %s.",
                    stageRunnerId, curStage.stageId, beforeSet));
            double runTime = CriticalPathUtil.getLongestTimeOfStage(curStage, cacheSpace); //run之前更新
            double contrastRunTime = CriticalPathUtil.getLongestTimeOfStage(curStage, null);// TODO: to delete for performance
            // after running stage, update MRDUtil's distance and LRCUtil's reference count
            cacheSpace.changeAfterStageRun(curStage);
            // end update
            // after running stage, add data into CacheSpace
            curStage.rdds.sort((o1, o2) -> (int) (o1.rddId - o2.rddId)); // TODO: check sorting effects
            for(RDD rdd : curStage.rdds) {
                // LRC是将所有的rdd都执行add操作
                if (hotRDDIdSet.contains(rdd.rddId) || cacheSpace.getPolicy() == ReplacePolicy.LRC) { // 不要重复添加 fix bug of repeatedly adding ` && !cacheSpace.getCachedRDDIds().contains(rdd.rddId)`
                    // for check
                    beforeSet = new HashSet<>(cacheSpace.getCachedRDDIds());
                    cacheSpace.addRDD(rdd); // 原本的代码
                    try {
                        if (hotRDDIdSet.contains(rdd.rddId)) {
                            ValidationUI.writeLine(SimulatorProcess.curJobId, curStage.stageId, rdd,
                                    beforeSet, cacheSpace.getCachedRDDIds(), beforePriority, cacheSpace.getPolicy(), cacheSpace.getPriority());
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    // end check
//                    cacheSpace.addRDD(rdd);
                }
            }
            // end add
            res += runTime;
            logger.info(String.format("StageRunner [%s] has run Stage [%d] for [%f]s, contrast for [%f]s, CacheSpace %s -> %s.",
                    stageRunnerId, curStage.stageId, runTime, contrastRunTime, beforeSet, cacheSpace.getCachedRDDIds()));
        }
        return res;
    }


}
