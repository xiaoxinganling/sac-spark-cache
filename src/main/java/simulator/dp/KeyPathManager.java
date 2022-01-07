package simulator.dp;

import entity.Job;
import entity.Stage;
import utils.CriticalPathUtil;

import java.util.List;
import java.util.Map;

/**
 * 生成每个job的关键路径——需要有路径的关键路径算法(from leetcode)
 */
public class KeyPathManager {

    public static Map<Long, Stage> generateKeyStages(List<Job> jobList) {
        Map<Long, Stage> keyStages = CriticalPathUtil.getKeyStagesOfJobList(jobList);
        for (Stage stage : keyStages.values()) {
            stage.rdds.sort((o1, o2) -> (int) (o2.rddId - o1.rddId));
        }
        return keyStages;
    }

    public static void updateKeyStages(Map<Long, Stage> keyStages, Stage stage) {
        keyStages.remove(stage.stageId);
    }

}
