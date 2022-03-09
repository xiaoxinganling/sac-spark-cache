package task.ds;

import entity.Partition;
import java.util.*;

public class SLRUUtil extends SReplaceUtil {

    private LinkedHashMap<String, Partition> memoryMap;

    public SLRUUtil() {
        memoryMap = new LinkedHashMap<>(16, 0.75f, true);
        cachedPartitionIds = new HashSet<>();
    }

    @Override
    public boolean addPartition(Partition partition) {
        String key = partition.getPartitionId();
        if (cachedPartitionIds.contains(key)) {
            getPartition(key);
            return false; // false to represent dup
        }
        memoryMap.put(key, partition);
        cachedPartitionIds.add(key);
        return true;
    }

    @Override
    public Partition deletePartition() {
        // delete with the order
        Iterator<Map.Entry<String, Partition>> iterator = memoryMap.entrySet().iterator();
        if (!iterator.hasNext()) {
            return null; // null to represent nothing to remove
        }
        Map.Entry<String, Partition> entryToDelete = iterator.next();
        memoryMap.remove(entryToDelete.getKey());
        cachedPartitionIds.remove(entryToDelete.getKey());
        return entryToDelete.getValue();
    }

    @Override
    public Set<String> getCachedPartitionIds() {
        return cachedPartitionIds;
    }

    @Override
    public Partition getPartition(String partitionId) {
        return memoryMap.get(partitionId);
    }

    @Override
    public void clear() {
        memoryMap.clear();
        cachedPartitionIds.clear();
    }
}
