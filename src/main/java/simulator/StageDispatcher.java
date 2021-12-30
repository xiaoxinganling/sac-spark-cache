package simulator;

import entity.Stage;
import lombok.Data;
import java.util.List;

@Data
public class StageDispatcher {

    private StageRunner[] stageRunners;

    public StageDispatcher(int runnerSize) {
        stageRunners = new StageRunner[runnerSize];
    }

    public void dispatchStage(List<Stage> submittedStage) {
        // 选择isUsing = false的Stage Runner
        // 如果所有Stage Runner已满，则按照轮询的方式为每个Stage Runner分配Stage
        int curToDispatch = 0;
        for(StageRunner sr : stageRunners) {
            if(!sr.isUsing) {

            }
        }
    }

}
