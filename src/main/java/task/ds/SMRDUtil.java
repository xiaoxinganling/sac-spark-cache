package task.ds;

import entity.Partition;
import entity.Task;
import simulator.RDDStageInfoManager;
import java.util.*;

public class SMRDUtil extends SReplaceUtil {

    private Map<String, PriorityQueue<Long>> partitionToTaskIds;

    private LinkedList<Partition> containsPartitions;

    public void setPartitionToTaskIds(Map<String, PriorityQueue<Long>> partitionToTaskIds) {
        this.partitionToTaskIds = partitionToTaskIds;
    }

    public SMRDUtil() {
        partitionToTaskIds = new HashMap<>(); // for the first preparation
        containsPartitions = new LinkedList<>();
        cachedPartitionIds = new HashSet<>();
    }

    public void updatePartitionDistanceByTask(Task task) {
        RDDStageInfoManager.updatePartitionDistance(partitionToTaskIds, task);
    }

    public void sortPartitionByDistance() {
        containsPartitions.sort((o1, o2) -> {
            int diff = (int) (partitionToTaskIds.get(o2.getPartitionId()).peek() - partitionToTaskIds.get(o1.getPartitionId()).peek());
            return diff != 0 ? diff : - o1.getPartitionId().compareTo(o2.getPartitionId());
        });
    }

    @Override
    public boolean addPartition(Partition partition) {
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
        return toDelete;
    }

    @Override
    public Set<String> getCachedPartitionIds() {
        return cachedPartitionIds;
    }

    @Override
    public Partition getPartition(String partitionId) {
        return null;
    }

    @Override
    public void clear() {
        partitionToTaskIds.clear();
        containsPartitions.clear();
        cachedPartitionIds.clear();
    }

}
