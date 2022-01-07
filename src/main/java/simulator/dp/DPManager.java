package simulator.dp;

import entity.RDD;
import entity.Stage;

import java.util.*;

/**
 * 根据每个RDD的空间、时间以及CacheSpace总大小得到最优缓存RDD list
 */
public class DPManager {

    // TODO: cachedRDD传进来之前先过滤掉0
    public static double chooseRDDToCache(Map<Long, Stage> keyStages, List<RDD> cachedRDD, int cacheSpaceSize, Set<Long> choseRDDIds) {
        int N = cachedRDD.size();
        Set<Long> cachedRDDIds = new HashSet<>();
        for (RDD rdd : cachedRDD) {
            cachedRDDIds.add(rdd.rddId);
        }
        Map<Long, Double> cachedTime = RDDTimeManager.cachedRDDTimeWithKeyStages(keyStages, cachedRDDIds);
        Map<String, Set<Long>> choice = new HashMap<>();
        double[][] dp = new double[N + 1][cacheSpaceSize + 1];
        for (int i = 1; i <= N; i++) {
            RDD curRDD = cachedRDD.get(i - 1);
            for (int w = 1; w <= cacheSpaceSize; w++) {
                if (w - curRDD.partitionNum < 0) {
                    dp[i][w] = dp[i - 1][w];
                    choice.put(String.format("%d_%d", i, w), new HashSet<>(
                            choice.getOrDefault(String.format("%d_%d", i - 1, w), new HashSet<>())));
                } else {
                    int beforeW = (int) (w - curRDD.partitionNum);
                    double chooseIt = dp[i - 1][beforeW] + cachedTime.getOrDefault(curRDD.rddId, 0.0);
                    double notChooseIt = dp[i - 1][w];
                    if (chooseIt > notChooseIt) {
                        Set<Long> tmp = new HashSet<>(choice.getOrDefault(String.format("%d_%d", i - 1, beforeW), new HashSet<>())); // fix 没有把long转为int
                        tmp.add(curRDD.rddId); // fix: 这里应该新建一个Set而不是直接用之前的引用
                        choice.put(String.format("%d_%d", i, w), tmp);
                        dp[i][w] = chooseIt;
                    } else {
                        choice.put(String.format("%d_%d", i, w),
                                new HashSet<>(choice.getOrDefault(String.format("%d_%d", i - 1, w), new HashSet<>())));
                        dp[i][w] = notChooseIt;
                    }
                }
            }
        }
        choseRDDIds.addAll(choice.getOrDefault(String.format("%d_%d", N, cacheSpaceSize), new HashSet<>()));
        return dp[N][cacheSpaceSize];
    }
}