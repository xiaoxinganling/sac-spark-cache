package entity.event.task;

import com.alibaba.fastjson.annotation.JSONField;
import entity.event.SparkListenerEvent;
import lombok.Data;

@Data
public class TaskEndEvent extends SparkListenerEvent {

    // TODO: 增加属性时记得更新’xxx_task‘文件

    @JSONField(name = "Stage ID")
    public Long stageId;

    @JSONField(name = "Task Info")
    public TaskInfo taskInfo;

    @JSONField(name = "Task Metrics")
    public TaskMetrics taskMetrics;

}
