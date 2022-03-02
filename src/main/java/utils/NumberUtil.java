package utils;

import java.util.List;

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

    public static double sum(List<Double> data) {
        double sum = 0;
        for (double d : data)
            sum += d;
        return sum;
    }

    public static double mean(List<Double> data) {
        if(data.size() == 0) {
            return 0;
        }
        return sum(data) / data.size();
    }

    // population variance 总体方差
    public static double variance(List<Double> data) {
        if(data.size() == 0) {
            return 0;
        }
        double variance = 0, meanData = mean(data);
        for (double d : data) {
            variance += (Math.pow((d - meanData), 2));
        }
        return variance / data.size();
    }

    // population standard deviation 总体标准差
    public static double stdDev(List<Double> data) {
        return Math.sqrt(variance(data));
    }

    // return the max value of list, if size == 0, return Integer.MIN_VALUE
    public static Double max(List<Double> data) {
        double max = Double.MIN_VALUE;
        for(double d : data) {
            max = Math.max(max, d);
        }
        return max;
    }

    // return the number * ratio with some limitations
    public static int numberWithRatio(int number, double ratio) {
        return (int) Math.ceil(number * ratio);
    }

}
