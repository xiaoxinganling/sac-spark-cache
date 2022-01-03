package simulator;

import entity.RDD;
import entity.Stage;
import lombok.Data;
import org.apache.log4j.Logger;
import utils.CriticalPathUtil;

import java.util.*;

@Data
public class StageRunner {

    public String stageRunnerId;

    private Queue<Stage> stageQueue;

    private CacheSpace cacheSpace;

    private List<RDD> hotRDD;

    private Set<Long> hotRDDIdSet;

    public List<RDD> getHotRDD() {
        return hotRDD;
    }

    public void setHotRDD(List<RDD> hotRDD) {
        this.hotRDD = hotRDD;
    }

    public Set<Long> getHotRDDIdSet() {
        return hotRDDIdSet;
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
            Stage curStage = stageQueue.poll();
            Set<Long> beforeSet = new HashSet<>(cacheSpace.getCachedRDDIds());
            logger.info(String.format("StageRunner [%s] is running Stage [%d] with CacheSpace %s.",
                    stageRunnerId, curStage.stageId, beforeSet));
            double runTime = CriticalPathUtil.getLongestTimeOfStage(curStage, cacheSpace);
            double contrastRunTime = CriticalPathUtil.getLongestTimeOfStage(curStage, null);// TODO: to delete for performance
            // after running stage, add data into CacheSpace
            curStage.rdds.sort((o1, o2) -> (int) (o1.rddId - o2.rddId));
            for(RDD rdd : curStage.rdds) {
                if (hotRDDIdSet.contains(rdd.rddId)) { // 不要重复添加 fix bug of repeatedly adding ` && !cacheSpace.getCachedRDDIds().contains(rdd.rddId)`
                    cacheSpace.addRDD(rdd);
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
