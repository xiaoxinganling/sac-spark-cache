package simulator;

public enum ReplacePolicy {
    LRU,
    LFU,
    LRC,
    MRD,
    DP,
    FIFO;

    public static void main(String[] args) {
        System.out.printf("%s", LRC);
    }

}
