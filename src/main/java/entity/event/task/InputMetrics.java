package entity.event.task;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

@Data
public class InputMetrics {

    @JSONField(name = "Bytes Read")
    public Long bytesRead;

}
