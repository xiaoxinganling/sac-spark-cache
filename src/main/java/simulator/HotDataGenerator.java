package simulator;

import entity.Job;
import entity.RDD;
import entity.Stage;
import entity.event.JobStartEvent;
import org.apache.log4j.Logger;
import utils.CacheSketcher;
import java.util.*;

public class HotDataGenerator {

    /**
     * 返回一个application所包含的jobList的 hot RDD
     * @param jobList
     * @param applicationName: only for logging
     * @return
     */

    public static Logger logger = Logger.getLogger(HotDataGenerator.class);

    public static List<RDD> hotRDD(String applicationName, List<Job> jobList) {
        List<JobStartEvent> jseList = new ArrayList<>(jobList);
        int[][] simpleDAG = CacheSketcher.generateSimpleDAGByJobsAndStages(jseList, null);
        Set<Long> hotRDDIds = new HashSet<>();
        for(int i = 0; i < simpleDAG.length - jobList.size(); i++) {
            int sum = 0;
            for(int j = 0; j < simpleDAG[0].length; j++) {
                if(simpleDAG[i][j] > 0) {
                    sum += 1;
                }
            }
            if(sum > 1) {
                hotRDDIds.add((long) i);
            }
        }
        logger.info(String.format("HotDataGenerator generates hot RDD %s of application [%s].",
                hotRDDIds, applicationName));
        // TODO: determine whether to return RDD or RDDId
        List<RDD> resRDD = new ArrayList<>();
        Set<Long> hasVisited = new HashSet<>();
        for(Job job : jobList) {
            for(Stage stage : job.stages) {
                for(RDD rdd : stage.rdds) {
                    if(hotRDDIds.contains(rdd.rddId) && !hasVisited.contains(rdd.rddId)) {
                        // 去重, 有些丑陋，但是用Set比用map更节省内存
                        resRDD.add(rdd);
                        hasVisited.add(rdd.rddId);
                    }
                }
            }
        }
        return resRDD;
    }

    public static List<RDD> twiceRDD(String applicationName, List<Job> jobList) {
        List<JobStartEvent> jseList = new ArrayList<>(jobList);
        Map<Long, Integer> directRefForAllJobs = CacheSketcher.generateRDDDirectRefForJobs(jseList, null);
        Set<Long> twiceRDDIds = new HashSet<>();
        for(Map.Entry<Long, Integer> entry : directRefForAllJobs.entrySet()) {
            if(entry.getValue() >= 2) {
                twiceRDDIds.add(entry.getKey());
            }
        }
        logger.info(String.format("HotDataGenerator generates twice RDD %s of application [%s].",
                twiceRDDIds, applicationName));
        // TODO: determine whether to return RDD or RDDId
        List<RDD> resRDD = new ArrayList<>();
        Set<Long> hasVisited = new HashSet<>();
        for(Job job : jobList) {
            for(Stage stage : job.stages) {
                for(RDD rdd : stage.rdds) {
                    if(twiceRDDIds.contains(rdd.rddId) && !hasVisited.contains(rdd.rddId)) {
                        // 去重, 有些丑陋，但是用Set比用map更节省内存
                        resRDD.add(rdd);
                        hasVisited.add(rdd.rddId);
                    }
                }
            }
        }
        return resRDD;
    }


    /**
     * 建议的CacheSpace大小，默认为所有hot data size / 2（向上取整）
     * @param application: for log only
     * @param hotData
     * @return
     */
    public static long proposeCacheSpaceSize(String application, List<RDD> hotData) {
        long totalSize = 0;
        for (RDD rdd : hotData) {
            totalSize += rdd.partitionNum;
//            System.out.println(rdd.rddId + " -> " + rdd.partitionNum);
        }
        long proposeSize = (totalSize & 0x1) == 0 ? totalSize / 2 : totalSize / 2 + 1;
        logger.info(String.format("HotDataGenerator: Proposed CacheSpace size for [%s] is [%d], average [%.2f].",
                application, proposeSize, totalSize / (double) hotData.size()));
        return proposeSize;
    }
}
