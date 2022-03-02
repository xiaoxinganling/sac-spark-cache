package entity.event.task;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

@Data
public class TaskInfo {

    @JSONField(name = "Task ID")
    public Long taskId;

    @JSONField(name = "Launch Time")
    public Long startTime;

    @JSONField(name = "Finish Time")
    public Long finishTime;

}
