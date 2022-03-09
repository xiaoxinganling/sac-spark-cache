package task.ds;

import entity.Partition;
import entity.Task;
import simulator.ReferenceCountManager;
import java.util.*;

public class SLRCUtil extends SReplaceUtil {

    private Map<String, Integer> hotPartitionRC;

    private Map<String, Integer> partitionIdToActionNum;

    private LinkedList<Partition> containsPartitions;

    public void setHotPartitionRC(Map<String, Integer> hotPartitionRC) {
        this.hotPartitionRC = hotPartitionRC;
    }

    public void setPartitionIdToActionNum(Map<String, Integer> partitionIdToActionNum) {
        this.partitionIdToActionNum = partitionIdToActionNum;
    }

    public SLRCUtil() {
        hotPartitionRC = new HashMap<>();
        partitionIdToActionNum = new HashMap<>();
        containsPartitions = new LinkedList<>();
        cachedPartitionIds = new HashSet<>();
    }

    @Override
    public boolean addPartition(Partition partition) {
        assert hotPartitionRC.containsKey(partition.getPartitionId());
        if (!hotPartitionRC.containsKey(partition.getPartitionId())) {
            // 已经没有ref count了
            return false;
        }
        containsPartitions.add(partition);
        cachedPartitionIds.add(partition.getPartitionId());
        return true;
    }

    @Override
    public Partition deletePartition() {
        Partition toDelete = containsPartitions.removeFirst();
        if (toDelete != null) {
            cachedPartitionIds.remove(toDelete.getPartitionId());
        }
        assert toDelete != null;
        return toDelete;
    }

    @Override
    public Set<String> getCachedPartitionIds() {
        return cachedPartitionIds;
    }

    @Override
    public Partition getPartition(String partitionId) {
        return null; // 不必实现
    }

    @Override
    public void clear() {
        hotPartitionRC.clear();
        partitionIdToActionNum.clear();
        containsPartitions.clear();
        cachedPartitionIds.clear();
    }

    public void sortPartitionByRC() {
        // o2.partitionId与o1待定
        containsPartitions.sort((o1, o2) -> {
            int compareRes = hotPartitionRC.get(o1.getPartitionId()) - hotPartitionRC.get(o2.getPartitionId());
            return  compareRes != 0 ? compareRes : o2.getPartitionId().compareTo(o1.getPartitionId());
        });
    }

    public void updateHotPartitionRefCountByTask(Task task) {
        // call function from ReferenceCountManager
        ReferenceCountManager.updateHotPartitionRefCountByTask(hotPartitionRC, task, partitionIdToActionNum);
    }

}
