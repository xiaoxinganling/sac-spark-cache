package entity;

import java.util.ArrayList;
import java.util.List;

/**
 * 表示RDD的每个block
 */
public class Partition{

    private String partitionId;

    private int partitionIndex;

    private double partitionComputeTime;

    public RDD belongRDD;

    private int memorySize;

    public int getMemorySize() {
        return memorySize;
    }

    public void setMemorySize(int memorySize) {
        this.memorySize = memorySize;
    }

    private List<String> parentIds;

    public void setParentIds(List<String> parentIds) {
        this.parentIds = parentIds;
    }

    public Partition(int partitionIndex, double partitionComputeTime, RDD belongRDD) {
        this.partitionIndex = partitionIndex;
        this.partitionComputeTime = partitionComputeTime;
        this.belongRDD = belongRDD;
        partitionId = String.format("%s_%s", belongRDD.rddId, partitionIndex);
        parentIds = new ArrayList<>();
        // situation 1: parent的partition数加起来=当前RDD的partition数 （只有SVM这么弄）
        // situation 2: parent的partition数和当前rdd的partition数都一样
        for (long rddParentId : belongRDD.rddParentIDs) {
            parentIds.add(String.format("%s_%s", rddParentId, partitionIndex)); // TODO: 之后算时间时需要考虑partition在不在这个task中
        }
    }

    public String getPartitionId() {
        return partitionId;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public double getPartitionComputeTime() {
        return partitionComputeTime;
    }

    public List<String> getParentIds() {
        return parentIds;
    }
}
