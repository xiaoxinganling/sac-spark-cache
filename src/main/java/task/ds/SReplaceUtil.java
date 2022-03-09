package task.ds;

import entity.Partition;
import java.util.Set;

public abstract class SReplaceUtil {

    protected Set<String> cachedPartitionIds;

    public abstract boolean addPartition(Partition partition);

    public abstract Partition deletePartition();

    public abstract Set<String> getCachedPartitionIds();

    public abstract Partition getPartition(String partitionId);

    public abstract void clear();

}
