package utils;

import entity.RDD;
import entity.Stage;

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

    // Perform DFS on graph and set departure time of all
    // vertices of the graph
    private static int DFS(Graph graph, int v, boolean[] discovered,
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

    public static double getLongestTimeOfStage(Stage stage) {
        // List of graph edges as per above diagram
        Set<Long> rddIdSet = new HashSet<>();
        for(RDD rdd : stage.rdds) {
            rddIdSet.add(rdd.rddId);
        }
        List<Edge> edges = new ArrayList<>();
        long maxId = 0;
        for(RDD rdd : stage.rdds) {
            maxId = Math.max(maxId, rdd.rddId);
            for(long parentId : rdd.rddParentIDs) {
                if(rddIdSet.contains(parentId)) {
                    edges.add(new Edge(rdd.rddId.intValue(), (int) parentId, 1));
                }
            }
        }

        int N = (int) maxId + 1;

        // create a graph from given edges
        Graph graph = new Graph(edges, N);

        int source = SimpleUtil.lastRDDOfStage(stage).rddId.intValue();

        // find longest distance of all vertices from given source
        return findLongestDistance(graph, source, N) + 1; // add initial compute time
    }

    public static void main(String[] args) {
//        // List of graph edges as per above diagram
//        List<Edge> edges = Arrays.asList(
//                new Edge(0, 1, 5),
//                new Edge(2, 3, 7),
//                new Edge(0, 2, 3),
//                new Edge(3, 5, 1),
//                new Edge(1, 3, 6),
//                new Edge(3, 4, -1),
//                new Edge(1, 2, 2),
//                new Edge(4, 5, -2),
//                new Edge(2, 4, 4),
//                new Edge(2, 5, 2)
//        );
//
//        // Set number of vertices in the graph
//        final int N = 6;
//
//        // create a graph from given edges
//        Graph graph = new Graph(edges, N);
//
//        // source vertex
//        int source = 1;
//
//        // find longest distance of all vertices from given source
//        findLongestDistance(graph, source, N);
    }
}

//Sources for code
// https://www.techiedelight.com/find-cost-longest-path-dag/
//https://www.geeksforgeeks.org/find-longest-path-directed-acyclic-graph/



