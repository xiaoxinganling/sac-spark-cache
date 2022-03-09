package simulator;

public enum ReplacePolicy {
    LRU,
    LFU,
    LRC,
    MRD,
    DP,
    FIFO,
    SLRU,
    SLRC,
    SMRD,
    SDP;

    public static void main(String[] args) {
        System.out.printf("%s", LRC);
    }

}
