package task.ds;

import entity.Partition;

import java.util.Set;

public class SDPUtil extends SReplaceUtil {
    @Override
    public boolean addPartition(Partition partition) {
        return false;
    }

    @Override
    public Partition deletePartition() {
        return null;
    }

    @Override
    public Set<String> getCachedPartitionIds() {
        return null;
    }

    @Override
    public Partition getPartition(String partitionId) {
        return null;
    }

    @Override
    public void clear() {

    }
}
