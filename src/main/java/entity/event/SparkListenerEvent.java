package entity.event;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

@Data
public abstract class SparkListenerEvent {

    @JSONField(name = "Event")
    public String eventName;

}
