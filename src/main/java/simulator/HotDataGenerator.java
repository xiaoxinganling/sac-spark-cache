package simulator;

import entity.*;
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

    public static List<RDD> hotRDD(String applicationName, List<Job> jobList, ReplacePolicy replacePolicy) {
//        if (replacePolicy != null) {// TODO: need to remove
//            if (replacePolicy != ReplacePolicy.DP) {
//                Map<Long, RDD> rddMap = new HashMap<>();
//                for (Job job : jobList) {
//                    for (Stage stage : job.stages) {
//                        for (RDD rdd : stage.rdds) {
//                            rddMap.putIfAbsent(rdd.rddId, rdd);
//                        }
//                    }
//                }
//                List<RDD> res = new ArrayList<>(rddMap.values());
//                return res;
//            }
//        }
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
//        long proposeSize = (totalSize & 0x1) == 0 ? totalSize / 2 : totalSize / 2 + 1;
        long proposeSize = totalSize; //直接传入总size
        logger.info(String.format("HotDataGenerator: Proposed CacheSpace size for [%s] is [%d], average [%.2f].",
                application, proposeSize, totalSize / (double) hotData.size()));
        return proposeSize;
    }

    public static long proposeTaskCacheSpace(String application, List<RDD> hotRDD, Map<Long, List<Task>> stageIdToTasks) {
        Set<Long> hotRDDIdSet = new HashSet<>();
        for (RDD rdd : hotRDD) {
            hotRDDIdSet.add(rdd.rddId);
        }
        // KEYPOINT: 使用Map去重
        Map<String, Partition> hotPartitionMap = HotDataGenerator.generateHotPartitionMap(hotRDDIdSet, stageIdToTasks);
        long totalSize = 0;
        for (Partition p : hotPartitionMap.values()) {
            totalSize += p.getMemorySize();
        }
        logger.info(String.format("HotDataGenerator: Proposed SCacheSpace size for [%s] is [%d], average [%.2f] of [%d] Partitions.",
                application, totalSize, totalSize / (double) hotPartitionMap.size(), hotPartitionMap.size()));
        return totalSize;
    }

    public static Map<String, Partition> generateHotPartitionMap(Set<Long> hotRDDIdSet, Map<Long, List<Task>> stageIdToTasks) {
        Map<String, Partition> hotPartitionMap = new HashMap<>();
        for (List<Task> tasks : stageIdToTasks.values()) {
            for (Task task : tasks) {
                for (Partition p : task.getPartitions()) {
                    if (hotRDDIdSet.contains(p.belongRDD.rddId)) {
                        hotPartitionMap.put(p.getPartitionId(), p);
                    }
                }
            }
        }
        return hotPartitionMap;
    }
}
