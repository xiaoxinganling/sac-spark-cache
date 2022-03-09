package entity;

import entity.event.task.TaskEndEvent;

import java.util.List;

public class Task extends TaskEndEvent {

    private long duration = -1;

    private List<Partition> partitions;

    private int needCPU;

    private int schedulePriority;

    public int getSchedulePriority() {
        return schedulePriority;
    }

    public void setSchedulePriority(int schedulePriority) {
        this.schedulePriority = schedulePriority;
    }

    public int getNeedCPU() {
        return needCPU;
    }

    public void setNeedCPU(int needCPU) {
        this.needCPU = needCPU;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public int getIndexInStage() {
        return indexInStage;
    }

    private int indexInStage;

    public void setPartitions(List<Partition> partitions) {
        this.partitions = partitions;
    }

    public void setIndexInStage(int indexInStage) {
        this.indexInStage = indexInStage;
    }

    public Long getTaskId() {
        return taskInfo.taskId;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public Long getDuration() {
        return (duration == -1) ? getActualDuration() : duration;
}

    public Long getMemory() {
        return taskMetrics.inputMetrics.bytesRead;
    }

    public Long getActualDuration() {
        return taskInfo.finishTime - taskInfo.startTime;
    }

}
