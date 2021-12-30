package utils;

public class UnionFindUtil {

    // 连通分量个数
    private int count;

    // 存储每个节点的父节点
    private int[] parent;

    // 记录每棵树的重量
    private int[] size;

    // initialize
    public UnionFindUtil(int n) {
        this.count = n;
        parent = new int[n];
        size = new int[n];
        for(int i = 0; i < n; i++) {
            parent[i] = i;
            size[i] = 1;
        }
    }

    // find root and compress path
    private int find(int x) {
        while(parent[x] != x) {
            // compress path
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        return x;
    }

    // count
    private int count(){
        return count;
    }

    // union
    public void union(int p, int q) {
        int rootP = find(p);
        int rootQ = find(q);
        if(rootP == rootQ) {
            return;
        }
        if(size[rootP] > size[rootQ]) {
            parent[rootQ] = rootP;
            size[rootP] += size[rootQ];
        }else{
            parent[rootP] = rootQ;
            size[rootQ] += size[rootP];
        }
        count--;
    }

    // connected
    public boolean connected(int p, int q) {
        int rootP = find(p);
        int rootQ = find(q);
        return rootP == rootQ;
    }

}
