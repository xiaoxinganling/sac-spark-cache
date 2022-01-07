package utils.ds;

import entity.RDD;
import entity.Stage;
import simulator.dp.DPManager;
import simulator.dp.KeyPathManager;
import simulator.dp.RDDTimeManager;

import java.util.*;

public class DPUtil extends ReplaceUtil {

    private Map<Long, RDD> containsRDD;

    private Map<Long, Stage> keyStages;

    public DPUtil() {
        containsRDD = new HashMap<>();
        keyStages = new HashMap<>();
    }

    public void updateKeyStagesByStage(Stage stage) {
        KeyPathManager.updateKeyStages(keyStages, stage);
    }

    public void setKeyStages(Map<Long, Stage> keyStages) {
        this.keyStages = keyStages;
    }

    public Set<Long> doDp(RDD curRDD, long cacheSize) {
        List<RDD> cachedRDDs = new ArrayList<>(containsRDD.values());
        cachedRDDs.add(curRDD);
        Set<Long> newCachedRDDIds = new HashSet<>();
        DPManager.chooseRDDToCache(keyStages, cachedRDDs, (int) cacheSize, newCachedRDDIds);
        return newCachedRDDIds;
    }

    @Override
    public long addRDD(RDD rdd) {
        if (containsRDD.containsKey(rdd.rddId)) {
            return 0;
        }
        containsRDD.put(rdd.rddId, rdd);
        return rdd.partitionNum;
    }

    @Override
    public RDD deleteRDD() {
        return null;
    }

    public RDD deleteRDD(long rddId) {
        return containsRDD.remove(rddId);
    }

    @Override
    public void clear() {
        containsRDD.clear();
        keyStages.clear();
    }

    @Override
    public Set<Long> getCachedRDDIds() {
        return containsRDD.keySet();
    }

    @Override
    public RDD getRDD(long rddId) {
        return null; // DP无需实现
    }

    @Override
    public Map getPriority() {
        return RDDTimeManager.cachedRDDTimeWithKeyStages(keyStages, getCachedRDDIds());
    }
}
