package entity.event.task;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

@Data
public class TaskMetrics {

    // TODO: use CPU time
    @JSONField(name = "Executor Deserialize CPU Time")
    public Long deserializeCPUTime;

    @JSONField(name = "Executor CPU Time")
    public Long CPUTime;

    @JSONField(name = "Input Metrics")
    public InputMetrics inputMetrics;

}
