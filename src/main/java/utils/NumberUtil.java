package utils;

public class NumberUtil {

    public static long comb(int m, int n) {
        if(m < n) {
            return 0;
        }
        if(n == 0) {
            return 1;
        }
        if(n == 1) {
            return m;
        }
        if(n > m / 2) {
            return comb(m, m - n);
        }
        if(n > 1) {
            return comb(m - 1, n - 1) + comb(m - 1, n);
        }
        return -1; // 无实际作用
    }

}
