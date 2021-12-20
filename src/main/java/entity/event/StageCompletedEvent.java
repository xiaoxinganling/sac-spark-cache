package entity.event;

import com.alibaba.fastjson.annotation.JSONField;
import entity.Stage;
import lombok.Data;

@Data
public class StageCompletedEvent extends SparkListenerEvent {

    @JSONField(name = "Stage Info")
    public Stage stage;

}
