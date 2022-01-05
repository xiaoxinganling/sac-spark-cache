package utils.ds;


import entity.RDD;
import java.util.*;

public class LRUUtil extends ReplaceUtil{

    private LinkedHashMap<Long, RDD> memoryMap;

    public LRUUtil() {
        memoryMap = new LinkedHashMap<>(16, 0.75f, true);
        cachedRDDIds = new HashSet<>();
    }

    @Override
    public long addRDD(RDD rdd) {
        if(cachedRDDIds.contains(rdd.rddId)) {
            getRDD(rdd.rddId); // 计算完毕还是需要更新一下的
            return 0;
        }
        memoryMap.put(rdd.rddId, rdd);
        cachedRDDIds.add(rdd.rddId);
        return rdd.partitionNum;
    }

    @Override
    public RDD deleteRDD() {
        // delete with the order
        Iterator<Map.Entry<Long, RDD>> iterator = memoryMap.entrySet().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        Map.Entry<Long, RDD> entryToDelete = iterator.next();
        memoryMap.remove(entryToDelete.getKey());
        cachedRDDIds.remove(entryToDelete.getKey());
        return entryToDelete.getValue();
    }

    @Override
    public void clear() {
        memoryMap.clear();
        cachedRDDIds.clear();
    }

    @Override
    public Set<Long> getCachedRDDIds() {
        return cachedRDDIds;
    }

    @Override
    public RDD getRDD(long rddId) {
        return memoryMap.get(rddId);
    }

    @Override
    public Map getPriority() {
        Map<Long, Integer> map = new HashMap<>();
        Iterator<Long> iterator = memoryMap.keySet().iterator();
        for (int i = 0; i < memoryMap.size(); i++) {
            map.put(iterator.next(), i + 1); // 越小优先级越低
        }
        return map;
    }
}
