package entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;
import java.util.List;

@Data
public class Stage {

    @JSONField(name = "Stage ID")
    public Long stageId;

    @JSONField(name = "Stage Attempt ID")
    public Long stageAttemptId;

    @JSONField(name = "Stage Name")
    public String stageName;

    @JSONField(name = "Number of Tasks")
    public Long taskNum;

    @JSONField(name = "RDD Info")
    public List<RDD> rdds;

    @JSONField(name = "Parent IDs")
    public List<Long> parentIDs;

    @JSONField(name = "Details")
    public String detail;

    @JSONField(name = "Submission Time")
    public Long submitTime;

    @JSONField(name = "Completion Time")
    public Long completeTime;

    @JSONField(name = "Accumulables")
    public List<Accumulable> accumulableList;

    public int needCPU;

}
