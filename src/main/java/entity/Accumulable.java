package entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

@Data
public class Accumulable {

    @JSONField(name = "ID")
    public Long id;

    @JSONField(name = "Name")
    public String name;

    @JSONField(name = "Value")
    public Long value;

    @JSONField(name = "Internal")
    public Boolean internal;

    @JSONField(name = "Count Failed Values")
    public Boolean countFailedValues;

}
