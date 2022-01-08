package simulator;

import entity.Job;
import entity.RDD;
import entity.Stage;
import lombok.Data;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
public class StageDispatcher {

    public String stageDispatcherId;

    private StageRunner[] stageRunners;

    private CacheSpace cacheSpace;

    private String curApplication;

    private List<Job> curJobList;

    private List<RDD> curHotData;

    private static int visitNum;

    private static int hitNum;

    public static double hitRatio() {
        return visitNum == 0 ? Integer.MAX_VALUE : hitNum / (double) visitNum;
    }

    public static void updateCacheHitRatio(List<Long> computePath, Set<Long> hotDataIdSet, Set<Long> cachedRDDIdSet) {
        // 因为第一次访问的时候不在，所以算非命中
        for (long rddId : computePath) {
            if (hotDataIdSet.contains(rddId)) {
                visitNum++;
            }
            if (cachedRDDIdSet.contains(rddId)) {
                hitNum++;
            }
        }
    }

    private Logger logger = Logger.getLogger(this.getClass());

    public void prepareForNewApplication(String curApplication, List<Job> curJobList, List<RDD> curHotData) {
        this.curApplication = curApplication;
        this.curJobList = curJobList;
        this.curHotData = curHotData;
        visitNum = 0;
        hitNum = 0;
    }

    public StageDispatcher(String stageDispatcherId, int runnerSize) {
        logger.info(String.format("StageDisPatcher [%s] is created.", stageDispatcherId));
        this.stageDispatcherId = stageDispatcherId;
        stageRunners = new StageRunner[runnerSize];
        for(int i = 0; i < runnerSize; i++) {
            stageRunners[i] = new StageRunner(String.format("SR-%s-%s", stageDispatcherId, i));
        }
    }

    public StageDispatcher(String stageDispatcherId, int runnerSize, CacheSpace cacheSpace) {
        logger.info(String.format("StageDisPatcher [%s] is created with CacheSpace [%s].",
                stageDispatcherId, cacheSpace.cacheSpaceId));
        this.stageDispatcherId = stageDispatcherId;
        stageRunners = new StageRunner[runnerSize];
        for(int i = 0; i < runnerSize; i++) {
            stageRunners[i] = new StageRunner(String.format("SR-%s-%s", stageDispatcherId, i), cacheSpace);
        }
        this.cacheSpace = cacheSpace;
    }

    // dispatch一次就run一次
    public void dispatchStage(List<Stage> submittedStage) {
        // 选择isUsing = false的Stage Runner
        // 如果所有Stage Runner已满，则按照轮询的方式为每个Stage Runner分配Stage
        StringBuilder stageIds = new StringBuilder();
        for(Stage stage : submittedStage) {
            stageIds.append(stage.stageId).append(",");
        }
        stageIds.deleteCharAt(stageIds.length() - 1);
        logger.info(String.format("StageDispatcher [%s] is dispatching Stages [%s] to StageRunners.", stageDispatcherId, stageIds.toString()));
        int curToDispatch = 0;
        for(StageRunner sr : stageRunners) {
            if(curToDispatch == submittedStage.size()) {
                break;
            }
            if(!sr.getIsUsing()) {
                sr.receiveStage(submittedStage.get(curToDispatch++));
            }
        }
        int stageRunnerIndex = 0;
        for(int i = curToDispatch; i < submittedStage.size(); i++) {
            stageRunners[stageRunnerIndex].receiveStage(submittedStage.get(curToDispatch));
            stageRunnerIndex = (stageRunnerIndex + 1) % stageRunners.length;
        }
        logger.info(String.format("StageDispatcher [%s] has dispatched Stages [%s] to StageRunners.", stageDispatcherId, stageIds.toString()));
    }

    public double runStages() {
        logger.info(String.format("StageDispatcher [%s] is instructing StageRunner to run Stages.", stageDispatcherId));
        double lastTime = 0;
        for(StageRunner sr : stageRunners) {
            lastTime = Math.max(lastTime, sr.runStages());
        }
        logger.info(String.format("StageDispatcher [%s] has instructed StageRunner to run Stages for [%f]s.",
                stageDispatcherId, lastTime));
        return lastTime;
    }

    public double runStagesWithCacheSpace() {
        logger.info(String.format("StageDispatcher [%s] is instructing StageRunner to run Stages with CacheSpace %s.",
                stageDispatcherId, cacheSpace.getCachedRDDIds()));
        double lastTime = 0;
        for(StageRunner sr : stageRunners) {
            lastTime = Math.max(lastTime, sr.runStagesWithCacheSpace(cacheSpace));
        }
        logger.info(String.format("StageDispatcher [%s] has instructed StageRunner to run Stages with CacheSpace %s for [%f]s.",
                stageDispatcherId, cacheSpace.getCachedRDDIds(), lastTime));
        return lastTime;
    }

    public void initializeHotRDDOfStageRunners() {
        Set<Long> hotRDDIdSet = new HashSet<>();
        for(RDD rdd : curHotData) {
            hotRDDIdSet.add(rdd.rddId);
        }
        for (StageRunner sr : stageRunners) {
            sr.setHotRDD(curHotData);
            sr.setHotRDDMap(curHotData);
            sr.setHotRDDIdSet(hotRDDIdSet);
            logger.info(String.format("StageDispatcher has updated hot RDD of StageRunner [%s] with %s.",
                    sr.stageRunnerId, hotRDDIdSet));
        }
    }

    public void initializeCacheSpace() {
        cacheSpace.prepare(this.curApplication, curJobList, curHotData);
        logger.info(String.format("StageDispatcher has clear CacheSpace for [%s].", this.curApplication));
    }

}
