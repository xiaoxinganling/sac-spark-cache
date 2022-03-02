package utils;


import java.util.*;

public class TopologicalUtil {

    public static List<Long> getTopologicalSortingOrder(Graph graph, int N) {
        Set<Integer> nodeSet = new HashSet<>();
        for (List<Edge> edges : graph.adjList) {
            for (Edge edge: edges) {
                nodeSet.add(edge.source);
                nodeSet.add(edge.dest);
            }
        }
        int[] departure = new int[N];
        Arrays.fill(departure, -1);
        boolean[] discovered = new boolean[N];
        int time = 0;
        for (int i = 0; i < N; i++) {
            if (!discovered[i]) {
                time = CriticalPathUtil.DFS(graph, i, discovered, departure, time);
            }
        }
        List<Long> topologicalOrder = new ArrayList<>();
        for (int i = N - 1; i >= 0; i--) {
            int v = departure[i];
            if (nodeSet.contains(v)) {
                topologicalOrder.add((long) v);
            }
        }
        return topologicalOrder;
    }
}
