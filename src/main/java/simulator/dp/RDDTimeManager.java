package simulator.dp;

import entity.RDD;
import entity.Stage;
import utils.CriticalPathUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 得到缓存RDD所节省的时间，关键类：CriticalPath
 */
public class RDDTimeManager {

    public static Map<String, Double> timeMemMap;

    // TODO: 可能非常耗时，待优化 -> yes
    public static Map<Long, Double> cachedRDDTimeWithKeyStages(Map<Long, Stage> keyStages, Set<Long> cachedRDD) {
        Map<Long, Double> futureRunTime = new HashMap<>();
        for (Stage stage : keyStages.values()) {
           // TODO: 假设keyStages中的RDD已经按照id从大到小排序
            for (RDD rdd : stage.rdds) {
                long key = rdd.rddId;
                if (cachedRDD.contains(key)) {
                    String timeKey = String.format("%d_%d", stage.stageId, key);
                    double timeReduced;
                    if (timeMemMap.containsKey(timeKey)) {
                        timeReduced = timeMemMap.get(timeKey);
                    } else {
                        timeReduced = CriticalPathUtil.getLongestTimeOfStageWithSource(stage, null, key, CriticalPathUtil.NO_NEED_FOR_PATH);
                        timeMemMap.put(timeKey, timeReduced);
                    }
//                    double timeReduced = 1;
//                    double timeReduced = CriticalPathUtil.getLongestTimeOfStageWithSource(stage, null, key);
                    futureRunTime.put(key, futureRunTime.getOrDefault(key, 0.0) + timeReduced);
                    break;
                }
            }
        }
        return futureRunTime;
    }

}
