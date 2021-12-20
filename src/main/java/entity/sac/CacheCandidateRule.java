package entity.sac;

import lombok.Data;

@Data
public class CacheCandidateRule {

    private float threshold;

    private boolean isDirect;

    // TODO: add greedy method
    private boolean isGreedy = false;

    public CacheCandidateRule(float threshold, boolean isDirect) {
        this.threshold = threshold;
        this.isDirect = isDirect;
    }

    public float getThreshold() {
        return threshold;
    }

    public boolean isDirect() {
        return isDirect;
    }

    public boolean isGreedy() {
        return isGreedy;
    }
}
