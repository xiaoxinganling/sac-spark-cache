package simulator;

import entity.Stage;
import lombok.Data;
import org.apache.log4j.Logger;

import java.util.List;

@Data
public class StageDispatcher {

    public String stageDispatcherId;

    private StageRunner[] stageRunners;

    private Logger logger = Logger.getLogger(this.getClass());

    public StageDispatcher(String stageDispatcherId, int runnerSize) {
        logger.info(String.format("StageDisPatcher [%s] is created.", stageDispatcherId));
        this.stageDispatcherId = stageDispatcherId;
        stageRunners = new StageRunner[runnerSize];
        for(int i = 0; i < runnerSize; i++) {
            stageRunners[i] = new StageRunner(String.format("SR-%s-%s", stageDispatcherId, i));
        }
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

    public double runStagesWithCacheSpace(CacheSpace cacheSpace) {
        logger.info(String.format("StageDispatcher [%s] is instructing StageRunner to run Stages with CacheSpace [%s].",
                stageDispatcherId, cacheSpace.getRddIds()));
        double lastTime = 0;
        for(StageRunner sr : stageRunners) {
            lastTime = Math.max(lastTime, sr.runStagesWithCacheSpace(cacheSpace));
        }
        logger.info(String.format("StageDispatcher [%s] has instructed StageRunner to run Stages with CacheSpace [%s] for [%f]s.",
                stageDispatcherId, cacheSpace.getRddIds(), lastTime));
        return lastTime;
    }

}
