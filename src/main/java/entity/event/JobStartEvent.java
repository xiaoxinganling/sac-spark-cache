package entity.event;

import com.alibaba.fastjson.annotation.JSONField;
import entity.Stage;
import lombok.Data;
import java.util.List;

@Data
public class JobStartEvent extends SparkListenerEvent{

    @JSONField(name = "Job ID")
    public Long jobId;

    @JSONField(name = "Submission Time")
    public Long submissionTime;

    @JSONField(name = "Stage Infos")
    public List<Stage> stages;

    @JSONField(name = "Stage IDs")
    public List<Long> stageIds;

    @JSONField(name = "Properties")
    public String properties;

}
