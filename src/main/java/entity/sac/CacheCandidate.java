package entity.sac;

import lombok.Data;

import java.util.List;

@Data
public class CacheCandidate {

    public long rddId;

    public int ref;

    public int directRef;

    public int hopComputeTime;

    public long partition;

    private Float ce = -1f; // log+1

    private Float directCe = -1f; //log+1

    // FIXME: parent补丁 for greedy method
    private List<Long> parentIds;

    public void setParent(List<Long> parentIds) {
        this.parentIds = parentIds;
    }

    public List<Long> getParentIds() {
        return parentIds;
    }

    public float getCe() {
        if(ce.equals(-1f)) {
            ce = (ref - 1) * (float) Math.log(hopComputeTime / (float) partition + 1);
        }
        return ce;
    }

    public float getDirectCe() {
        if(directCe.equals(-1f)) {
            directCe = (directRef - 1) * (float) Math.log(hopComputeTime / (float) partition + 1);
        }
        return directCe;
    }

    public CacheCandidate(long rddId, int ref, int directRef, int hopComputeTime, long partition) {
        this.rddId = rddId;
        this.ref = ref;
        this.directRef = directRef;
        this.hopComputeTime = hopComputeTime;
        this.partition = partition;
    }
}
