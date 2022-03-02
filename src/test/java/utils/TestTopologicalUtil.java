package utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TestTopologicalUtil {

    @Test
    void getTopologicalSortingOrder() {
        {
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
            final int N = 6;
            Graph graph = new Graph(edges, N);
            System.out.println(TopologicalUtil.getTopologicalSortingOrder(graph, N));
        }
        {
            List<Edge> edges = Arrays.asList(
                    new Edge(3, 8, 5),
                    new Edge(3, 10, 7),
                    new Edge(7, 11, 3),
                    new Edge(7, 8, 1),
                    new Edge(5, 11, 6),
                    new Edge(11, 2, -1),
                    new Edge(11, 9, 2),
                    new Edge(11, 10, -2),
                    new Edge(8, 9, 4)
            );
            final int N = 12;
            Graph graph = new Graph(edges, N);
            System.out.println(TopologicalUtil.getTopologicalSortingOrder(graph, N));
        }
    }
}