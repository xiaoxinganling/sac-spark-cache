package utils;

/*
Pseudo Code
Floyd-Warshall(W) {
	n = rows[W]
	for i=1 to n do
	       for j = 1 to n do
	              D[i,j] = W[i,j]

	for k=1 to n do
	       for i=1 to n do
	             for j=1 to n do
		     if (D[i,k]+D[k,j] < D[i,j])
			D[i,j]=D[i,k]+D[k,j]

     return D
}
*/
public class FloydUtil {
    public static int I = 99999999;//represents INFINITY

    public static int[][] FloydWarshall(int[][] w) {
        int n = w.length;
        int i, j, k;
        // Floyd-Warshall Algorithm O(n^3)
        for (k = 0; k < n; k++)
            for (j = 0; j < n; j++)
                for (i = 0; i < n; i++)
                    // If vertex k is on the shortest path from
                    // i to j, then update the value of dist[i][j]
                    if (w[i][k] + w[k][j] < w[i][j]) {
                        w[i][j] = w[i][k] + w[k][j];
                    }
//        printMatrix(dist);
        return w;
    }

    public static void main(String[] args) {
        // Assume an adjacency matrix representation
        // Assume vertices are numbered 1,2,…,n
        // The input is a n x n matrix (see README.md)
        int[][] graph2 = {
                {0, 1, I},
                {I, 0, I},
                {I, 1, 0}
        };
        int[][] graph3 = {
                { 0, 3, 8, I, -4 },
                { I, 0, I, 1, 7 },
                { I, 4, 0, I, I },
                { 2, I, -5, 0, I },
                { I, I, I, 6, 0 }
        };
        printMatrix(FloydWarshall(graph2));
        printMatrix(FloydWarshall(graph3));
    }
    public static void printMatrix(int[][] dist) {
        int n = dist.length;
        System.out.println("New Matrix: ");
        for (int[] ints : dist) {
            for (int j = 0; j < n; ++j) {
                if (ints[j] == I)
                    System.out.print("I   \t");
                else
                    System.out.print(ints[j] + "   \t");
            }
            System.out.println();
        }
    }
}

/*
OUTPUT:
New Matrix:
0   	1   	-3   	2   	-4
3   	0   	-4   	1   	-1
7   	4   	0   	5   	3
2   	-1   	-5   	0   	-2
8   	5   	1   	6   	0

*/