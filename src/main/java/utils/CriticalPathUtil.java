package utils;

import entity.Job;
import entity.RDD;
import entity.Stage;
import simulator.CacheSpace;
import simulator.ReplacePolicy;
import sketch.StaticSketch;

import java.util.*;


// Data structure to store graph edges
class Edge
{
    int source, dest;
    double weight;

    public Edge(int source, int dest, double weight) {
        this.source = source;
        this.dest = dest;
        this.weight = weight;
    }
};

// Class to represent a graph object
class Graph
{
    // A List of Lists to represent an adjacency list
    List<List<Edge>> adjList = null;

    // Constructor
    Graph(List<Edge> edges, int N) {
        adjList = new ArrayList<>(N);

        for (int i = 0; i < N; i++) {
            adjList.add(i, new ArrayList<>());
        }

        // add edges to the undirected graph
        for (Edge edge: edges) {
            adjList.get(edge.source).add(edge);
        }
    }
}

public class CriticalPathUtil {

    public static final long STAGE_LAST_NODE = Long.MAX_VALUE;

    public static final List<Long> NO_NEED_FOR_PATH = null;

    // Perform DFS on graph and set departure time of all
    // vertices of the graph
    public static int DFS(Graph graph, int v, boolean[] discovered,
                           int[] departure, int time) {
        // mark current node as discovered
        discovered[v] = true;

        // set arrival time - not needed
        // time++;

        // do for every edge (v -> u)
        for (Edge e : graph.adjList.get(v))
        {
            int u = e.dest;
            // u is not discovered
            if (!discovered[u]) {
                time = DFS(graph, u, discovered, departure, time);
            }
        }

        // ready to backtrack
        // set departure time of vertex v
        departure[time] = v;
        time++;

        return time;
    }

    // The function performs topological sort on a given DAG and then finds out
    // the longest distance of all vertices from given source by running
    // one pass of Bellman-Ford algorithm on edges of vertices in topological order
    private static double findLongestDistance(Graph graph, int source, int N) {
        // departure[] stores vertex number having its departure
        // time equal to the index of it
        int[] departure = new int[N];
        Arrays.fill(departure, -1);

        // stores vertex is discovered or not
        boolean[] discovered = new boolean[N];
        int time = 0;

        // perform DFS on all undiscovered vertices
        for (int i = 0; i < N; i++) {
            if (!discovered[i]) {
                time = DFS(graph, i, discovered, departure, time);
            }
        }

        double[] cost = new double[N];
        Arrays.fill(cost, Integer.MAX_VALUE);

        cost[source] = 0;

        // Process the vertices in topological order i.e. in order
        // of their decreasing departure time in DFS
        for (int i = N - 1; i >= 0; i--)
        {
            // for each vertex in topological order,
            // relax cost of its adjacent vertices
            int v = departure[i];
            for (Edge e : graph.adjList.get(v))
            {
                // edge e from v to u having weight w
                int u = e.dest;
                double w = e.weight * -1;	// negative the weight of edge

                // if the distance to the destination u can be shortened by
                // taking the edge vu, then update cost to the new lower value
                if (cost[v] != Integer.MAX_VALUE && cost[v] + w < cost[u]) {
                    cost[u] = cost[v] + w;
                }
            }
        }

//        System.out.println("The longest distances from source vertex: " + source);
        // print longest paths
        double max = 0;
        for (int i = 0; i < cost.length; i++) {
//            if(cost[i] * -1 == -2147483647){
//                System.out.printf("dist(%d, %d) = Infinity\n", source, i);
//            }
//            else{
//                System.out.printf("dist(%d, %d) = %2d\n", source, i, cost[i] * -1);
//            }
            if(cost[i] != Integer.MAX_VALUE) {
                max = Math.max(max, cost[i] * -1);
                // System.out.printf("dist(%d, %d) = %2f\n", source, i, cost[i] * - 1);
            }
        }
        return max;
    }

    private static double findLongestDistanceWithPath(Graph graph, int source, int N, Map<Long, Long> parentMap) {
        // departure[] stores vertex number having its departure
        // time equal to the index of it
        int[] departure = new int[N];
        Arrays.fill(departure, -1);

        // stores vertex is discovered or not
        boolean[] discovered = new boolean[N];
        int time = 0;

        // perform DFS on all undiscovered vertices
        for (int i = 0; i < N; i++) {
            if (!discovered[i]) {
                time = DFS(graph, i, discovered, departure, time);
            }
        }

        double[] cost = new double[N];
        Arrays.fill(cost, Integer.MAX_VALUE);

        cost[source] = 0;

        // Process the vertices in topological order i.e. in order
        // of their decreasing departure time in DFS
        for (int i = N - 1; i >= 0; i--)
        {
            // for each vertex in topological order,
            // relax cost of its adjacent vertices
            int v = departure[i];
            for (Edge e : graph.adjList.get(v))
            {
                // edge e from v to u having weight w
                int u = e.dest;
                double w = e.weight * -1;	// negative the weight of edge
                // if the distance to the destination u can be shortened by
                // taking the edge vu, then update cost to the new lower value
                if (cost[v] != Integer.MAX_VALUE && cost[v] + w < cost[u]) {
                    cost[u] = cost[v] + w;
                    parentMap.put((long) u, (long) v);
                }
            }
        }

//        System.out.println("The longest distances from source vertex: " + source);
        // print longest paths
        double max = 0;
        for (int i = 0; i < cost.length; i++) {
//            if(cost[i] * -1 == -2147483647){
//                System.out.printf("dist(%d, %d) = Infinity\n", source, i);
//            }
//            else{
//                System.out.printf("dist(%d, %d) = %2d\n", source, i, cost[i] * -1);
//            }
            if(cost[i] != Integer.MAX_VALUE) {
                max = Math.max(max, cost[i] * -1);
                // System.out.printf("dist(%d, %d) = %2f\n", source, i, cost[i] * - 1);
            }
        }
        return max;
    }

    // TODO: 这里是否要改呢？
    public static double getLongestTimeOfStageWithSource(Stage stage, CacheSpace cacheSpace,
                                                         long source, List<Long> computePath) {
        // List of graph edges as per above diagram
        Set<Long> rddIdSet = new HashSet<>();
        long maxId = 0;
        for(RDD rdd : stage.rdds) {
            rddIdSet.add(rdd.rddId);
            maxId = Math.max(maxId, rdd.rddId);
        }
        int N = (int) maxId + 2;
        // find longest distance of all vertices from given source
        if (source == STAGE_LAST_NODE) {
            source = SimpleUtil.lastRDDOfStage(stage).rddId;
        }

        // record path
        if (computePath != null) {
            Map<Long, Long> parentMap = new HashMap<>();
            double computeTime = getLongestTimeOfStageWithPath(stage, cacheSpace, parentMap);
            UnionFindUtil ufu = new UnionFindUtil(N);
            // start topological sort
            List<Edge> edges = new ArrayList<>();
            for (Map.Entry<Long, Long> entry : parentMap.entrySet()) {
                ufu.union(entry.getKey().intValue(), entry.getValue().intValue()); //直接union
                edges.add(new Edge(entry.getKey().intValue(), entry.getValue().intValue(), 1));
            }
            List<Long> tmpRDDIds = TopologicalUtil.getTopologicalSortingOrder(new Graph(edges, N), N);
            for (long rddId : tmpRDDIds) {
                if (ufu.connected((int) rddId, (int) source) && rddId != maxId + 1) {
                    computePath.add(rddId);
                }
            }
            // sort rdd => 这个优化是不符合规律的 => 其实应该选取拓扑序列(先用拓扑序列让你死的明白)
//            List<RDD> tmpRDDs  = new LinkedList<>(stage.rdds);
//            tmpRDDs.sort((o1, o2) -> (int) (o2.rddId - o1.rddId));
            // end sort
//            for (int i = tmpRDDs.size() - 1; i >= 0; i--) { // 这里是从小到大添加compute path的
//                RDD rdd = tmpRDDs.get(i);
//                if (ufu.connected(rdd.rddId.intValue(), (int) source) && rdd.rddId != maxId + 1) {
//                    computePath.add(rdd.rddId);
//                }
//            }
            return computeTime; // fix: add for longest path
        }
        // end record path

        List<Edge> edges = new ArrayList<>();
//        stage.rdds.sort((o1, o2) -> (int) (o2.rddId - o1.rddId)); // TODO: 这里存在降序排序
        if (cacheSpace != null && (cacheSpace.getPolicy() == ReplacePolicy.LRU || cacheSpace.getPolicy() == ReplacePolicy.LFU)) {
            stage.rdds.sort((o1, o2) -> (int) (o1.rddId - o2.rddId));
        }
        for(RDD rdd : stage.rdds) {
            if(cacheSpace != null && cacheSpace.rddInCacheSpace(rdd.rddId)) {
                continue;
            }
            int rddParentSize = 0;
            for(long parentId : rdd.rddParentIDs) {
                if(rddIdSet.contains(parentId)) {
                    edges.add(new Edge(rdd.rddId.intValue(), (int) parentId, rdd.computeTime)); // TODO: before 1
                    rddParentSize++;
                }
            }
            if(rddParentSize == 0) {
                edges.add(new Edge(rdd.rddId.intValue(), (int) maxId + 1, rdd.computeTime));
            }
        }

        // create a graph from given edges
        Graph graph = new Graph(edges, N);

        return findLongestDistance(graph, (int) source, N); // add initial compute time
    }

    /**
     * return <key,value> => <RDD, RDD.Parent>
     * @param stage
     * @param cacheSpace
     * @return
     */
    public static double getLongestTimeOfStageWithPath(Stage stage, CacheSpace cacheSpace, Map<Long, Long> parentMap) {
        // List of graph edges as per above diagram
        Set<Long> rddIdSet = new HashSet<>();
        long maxId = 0;
        for(RDD rdd : stage.rdds) {
            rddIdSet.add(rdd.rddId);
            maxId = Math.max(maxId, rdd.rddId);
        }
        List<Edge> edges = new ArrayList<>();
//        stage.rdds.sort((o1, o2) -> (int) (o2.rddId - o1.rddId)); // TODO: 这里存在降序排序
        if (cacheSpace != null && (cacheSpace.getPolicy() == ReplacePolicy.LRU || cacheSpace.getPolicy() == ReplacePolicy.LFU)) {
            stage.rdds.sort((o1, o2) -> (int) (o1.rddId - o2.rddId));
        }
        for(RDD rdd : stage.rdds) {
            if(cacheSpace != null && cacheSpace.rddInCacheSpace(rdd.rddId)) {
                continue;
            }
            int rddParentSize = 0;
            for(long parentId : rdd.rddParentIDs) {
                if(rddIdSet.contains(parentId)) {
                    edges.add(new Edge(rdd.rddId.intValue(), (int) parentId, rdd.computeTime));
                    rddParentSize++;
                }
            }
            if(rddParentSize == 0) {
                edges.add(new Edge(rdd.rddId.intValue(), (int) maxId + 1, rdd.computeTime));
            }
        }

        int N = (int) maxId + 2;

        // create a graph from given edges
        Graph graph = new Graph(edges, N);

        int source = SimpleUtil.lastRDDOfStage(stage).rddId.intValue();



        // find longest distance of all vertices from given source
        double runTime = findLongestDistanceWithPath(graph, source, N, parentMap); // add initial compute time
        if (runTime == 0) {
            // 特殊情况: stage的执行时间为0
            assert parentMap.size() == 0;
            parentMap.put(SimpleUtil.lastRDDOfStage(stage).rddId, maxId + 1);
        }
        return runTime;
    }

    /**
     * 返回一个job的key stages——打算用增量而不是全量的方式实现
     * @param job
     * @return
     */
    public static Map<Long, Stage> getKeyStagesOfJob(Job job) {
        Map<Long, Stage> stageMap = new HashMap<>();
        long maxId = 0;
        for (Stage stage : job.stages) {
            stageMap.put(stage.stageId, stage);
            maxId = Math.max(maxId, stage.stageId);
        }
        List<Edge> edges = new ArrayList<>();
        for (Stage stage : job.stages) {
            int stageParentSize = 0;
            for (long parentId : stage.parentIDs) {
                if (stageMap.containsKey(parentId)) {
                    edges.add(new Edge(stage.stageId.intValue(), (int) parentId,
                            getLongestTimeOfStageWithSource(stageMap.get(parentId), null, CriticalPathUtil.STAGE_LAST_NODE, CriticalPathUtil.NO_NEED_FOR_PATH))); // fix: 权重不能是1，应当是parent的最大路径
                    stageParentSize++;
                }
            }
            if (stageParentSize == 0) {
                edges.add(new Edge(stage.stageId.intValue(), (int) maxId + 1, 1)); // fix: 这里1倒是无所谓
            }
        }
        int N = (int) maxId + 2;
        Graph graph = new Graph(edges, N);
        int source = SimpleUtil.lastStageOfJob(job).stageId.intValue();
        Map<Long, Long> parentMap = new HashMap<>();
        findLongestDistanceWithPath(graph, source, N, parentMap);
        Map<Long, Stage> keyStageOfJob = new HashMap<>();
        long start = maxId + 1;
        while (parentMap.containsKey(start)) {
            long stageToAddId = parentMap.get(start);
            keyStageOfJob.put(stageToAddId, stageMap.get(stageToAddId)); // fix: 直接死循环
//            System.out.println("add: " + stageToAddId);
            start = stageToAddId;
        }
        return keyStageOfJob;
    }

    /**
     * 返回job list的所有key stages
     * @param jobList
     * @return
     */
    public static Map<Long, Stage> getKeyStagesOfJobList(List<Job> jobList) {
        Map<Long, Stage> keyStageMap = new HashMap<>(); // TODO: judge
        for (Job job : jobList) {
            Map<Long, Stage> keyStageForOneJob = getKeyStagesOfJob(job);
//            keyStageMap.putAll(keyStageForOneJob); // 不同job的stage不可能重复
            for (Map.Entry<Long, Stage> entry : keyStageForOneJob.entrySet()) {
                assert !keyStageMap.containsKey(entry.getKey());
                keyStageMap.put(entry.getKey(), entry.getValue());
            }
        }
        return keyStageMap;
    }

    public static void main(String[] args) {
        // List of graph edges as per above diagram
        List<Edge> edges = Arrays.asList(
                new Edge(0, 1, 5),
                new Edge(2, 3, 7),
                new Edge(0, 2, 3),
                new Edge(3, 5, 1),
                new Edge(1, 3, 6),
                new Edge(3, 4, -1),
                new Edge(1, 2, 2),
                new Edge(4, 5, -2),
                new Edge(2, 4, 4),
                new Edge(2, 5, 2)
        );

        // Set number of vertices in the graph
        final int N = 6;

        // create a graph from given edges
        Graph graph = new Graph(edges, N);

        // source vertex
        int source = 1;

        // find longest distance of all vertices from given source
        findLongestDistance(graph, source, N);
    }
}

//Sources for code
// https://www.techiedelight.com/find-cost-longest-path-dag/
//https://www.geeksforgeeks.org/find-longest-path-directed-acyclic-graph/



