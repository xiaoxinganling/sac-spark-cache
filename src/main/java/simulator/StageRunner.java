package simulator;

import entity.Stage;
import lombok.Data;
import org.apache.log4j.Logger;
import utils.CriticalPathUtil;
import java.util.LinkedList;
import java.util.Queue;

@Data
public class StageRunner {

    private String stageRunnerId;

    private Queue<Stage> stageQueue;

    private Logger logger = Logger.getLogger(this.getClass());

    public StageRunner(String stageRunnerId) {
        logger.info(String.format("StageRunner [%s] is created.", stageRunnerId));
        this.stageRunnerId = stageRunnerId;
        stageQueue = new LinkedList<>();
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
            logger.info(String.format("StageRunner [%s] is running Stage [%d] with CacheSpace [%s].",
                    stageRunnerId, curStage.stageId, cacheSpace.getRddIds()));
            double runTime = CriticalPathUtil.getLongestTimeOfStage(curStage, cacheSpace);
            res += runTime;
            logger.info(String.format("StageRunner [%s] has run Stage [%d] with CacheStage [%s] for [%f]s.",
                    stageRunnerId, curStage.stageId, cacheSpace.getRddIds(), runTime));
        }
        return res;
    }


}
