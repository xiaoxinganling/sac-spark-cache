import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import utils.UnionFindUtil;
import java.util.ArrayList;
import java.util.List;

public class TestUnionFindUtil {

    @Test
    void testUnionFind() {
        int[] a = {2, 3, 10};
        int[] b = {30, 38};
        int[] c = {27};
        int[] d = {2, 10};
        int[] e = {3, 10};
        int[][] total = {a, b, c, d, e};
        UnionFindUtil uf = new UnionFindUtil(39);
        for(int i = 0; i < total.length; i++) {
            for(int j = 0; j < total[i].length - 1; j++) {
                uf.union(total[i][j], total[i][j + 1]);
            }
        }
        assertTrue(uf.connected(2, 3));
        assertTrue(uf.connected(2, 10));
        assertTrue(uf.connected(3, 10));
        assertTrue(uf.connected(30, 38));
        assertFalse(uf.connected(27, 3));
        List<Integer> numSet = new ArrayList<>();
        for (int[] ints : total) {
            for (int anInt : ints) {
                if (!numSet.contains(anInt)) {
                    numSet.add(anInt);
                }
            }
        }
        for(int i = 0; i < numSet.size(); i++) {
            for (int j = i + 1; j < numSet.size(); j++) {
                System.out.println(numSet.get(i) + " " + numSet.get(j) + " --> "
                        + uf.connected(numSet.get(i), numSet.get(j)));
            }
        }
    }

}
