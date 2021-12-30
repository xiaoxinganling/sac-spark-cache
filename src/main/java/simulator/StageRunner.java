package simulator;

import entity.RDD;
import entity.Stage;
import lombok.Data;
import utils.SimpleUtil;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

@Data
public class StageRunner {

    public boolean isUsing;

    private Queue<Stage> stageQueue;

    public StageRunner() {
        isUsing = false;
        stageQueue = new LinkedList<>();
    }

    public boolean getIsUsing() {
        return stageQueue.size() > 0;
    }

    public void receiveStage(Stage stage) {
        stageQueue.offer(stage);
    }

    // return the runtime of stage queue
    public double runStages() {
        double res = 0;
        while(!stageQueue.isEmpty()) {
            res += runTimeOfStage(stageQueue.poll());
        }
        return res;
    }

    private double runTimeOfStage(Stage stage) {
        RDD lastRDD = SimpleUtil.lastRDDOfStage(stage);
        Map<Long, RDD> rddMap = new HashMap<>();
        for(RDD rdd : stage.rdds) {
            rddMap.put(rdd.rddId, rdd);
        }
        return SimpleUtil.lastRDDTimeOfStage(rddMap, lastRDD);
    }

}
