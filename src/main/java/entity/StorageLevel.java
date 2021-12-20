package entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

@Data
public class StorageLevel {

    @JSONField(name = "Use Disk")
    public Boolean useDisk;

    @JSONField(name = "Use Memory")
    public Boolean useMemory;

    @JSONField(name = "Deserialized")
    public Boolean deserialized;

    @JSONField(name = "Replication")
    public Long replication;

}
