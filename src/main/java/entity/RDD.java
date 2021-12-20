package entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;
import java.util.List;

@Data
public class RDD {

    @JSONField(name = "RDD ID")
    public Long rddId;

    @JSONField(name = "Name")
    public String rddName;

    @JSONField(name = "Scope")
    public String rddScope;

    @JSONField(name = "Callsite")
    public String rddCallsite;

    @JSONField(name = "Parent IDs")
    public List<Long> rddParentIDs;

    @JSONField(name = "Storage Level")
    public StorageLevel storageLevel;

    @JSONField(name = "Number of Partitions")
    public Long partitionNum;

    @JSONField(name = "Number of Cached Partitions")
    public Long cachedPartitionNum;

    @JSONField(name = "Memory Size")
    public Long memorySize;

    @JSONField(name = "Disk Size")
    public Long diskSize;
}
