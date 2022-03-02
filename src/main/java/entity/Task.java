package entity;

import entity.event.task.TaskEndEvent;

public class Task extends TaskEndEvent {

    private long duration = -1;

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
