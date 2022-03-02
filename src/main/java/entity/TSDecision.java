package entity;

// task schedule decision:
// schedule to a specific TaskRunner's executing Queue
public class TSDecision {

    public long taskId;

    public int taskRunnerIndex;

    public int queueIndex;

    public TSDecision(long taskId, int taskRunnerIndex, int queueIndex) {
        this.taskId = taskId;
        this.taskRunnerIndex = taskRunnerIndex;
        this.queueIndex = queueIndex;
    }

    @Override
    public String toString() {
        return String.format("[T:%d -> (TR:%d, Q:%d)]",
                taskId, taskRunnerIndex, queueIndex);
    }
}
