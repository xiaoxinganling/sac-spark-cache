package simulator;

import entity.*;
import entity.event.JobStartEvent;
import utils.CacheSketcher;
import utils.CriticalPathUtil;
import utils.SimpleUtil;
import java.util.*;

/**
 * 采用twice data的思路生成reference count
 */
public class ReferenceCountManager {

    // record rdd map of an application (job list)
    private static Map<Long, RDD> rddMapForSchedule;

    // record hot partition ids of application
    private static Set<String> hotPartitionIdsForSchedule;

    // record hot rdd ids of application
    private static Set<Long> hotRDDIdsForSchedule;

    private static Map<Long, Stage> stageMapForSchedule;

    private static Set<Long> lastStageIdsOfJobsForSchedule;

    private static Map<Long, Set<Long>> stageIdToRDDIdSetForSchedule;

    public static Map<Long, Integer> generateRefCountForHotData(List<Job> jobList, List<RDD> hotData) {
        Map<Long, Integer> hotDataRC = new HashMap<>();
        Set<Long> hotDataIds = new HashSet<>();
        List<JobStartEvent> jseList = new ArrayList<>(jobList);
        for (RDD rdd : hotData) {
            hotDataIds.add(rdd.rddId);
        } // TODO: 重复计算，待优化
        for (Map.Entry<Long, Integer> entry : CacheSketcher.generateRDDDirectRefForJobs(jseList, null).entrySet()) {
            if (hotDataIds.contains(entry.getKey())) {
                hotDataRC.put(entry.getKey(), entry.getValue());
            }
        }
        return hotDataRC;
    }

    public static Map<String, Integer> generateRefCountForHotPartition(List<Job> jobList, List<Partition> hotPartitions,
                                                                     Map<Long, List<Task>> stageIdToTasks) {
        Map<String, Integer> hotPartitionRC = new HashMap<>();
//        Map<Long, Set<Long>> stageIdToRDDIds = new HashMap<>();
//        Map<Long, Stage> stageMap = new HashMap<>();
        rddMapForSchedule = new HashMap<>();
        stageMapForSchedule = new HashMap<>();
        lastStageIdsOfJobsForSchedule = new HashSet<>();
        stageIdToRDDIdSetForSchedule = new HashMap<>();
        for (Job job : jobList) {
            lastStageIdsOfJobsForSchedule.add(SimpleUtil.lastStageOfJob(job).stageId);
            for (Stage stage : job.stages) {
                Set<Long> rddIds = new HashSet<>();
                stageMapForSchedule.putIfAbsent(stage.stageId, stage);
                for (RDD rdd : stage.rdds) {
                    rddMapForSchedule.putIfAbsent(rdd.rddId, rdd);
                    rddIds.add(rdd.rddId);
                }
                stageIdToRDDIdSetForSchedule.put(stage.stageId, rddIds);
            }
        }
        hotPartitionIdsForSchedule = new HashSet<>();
        hotRDDIdsForSchedule = new HashSet<>();
        for (Partition p : hotPartitions) {
            hotPartitionIdsForSchedule.add(p.getPartitionId());
            hotRDDIdsForSchedule.add(Long.parseLong(p.getPartitionId().split("_")[0]));
        }
        // 1. 第一类rc 四重for循环
        // 2. 第二类rc 跨stage间的rc 五重for循环
        for (Map.Entry<Long, List<Task>> entry : stageIdToTasks.entrySet()) {
            long stageId = entry.getKey();
            List<Task> tasks = entry.getValue();
            for (Task t : tasks) {
                for (Partition p : t.getPartitions()) {
                    boolean needToConsiderOverStage = true;
                    for (String parentId : p.getParentIds()) {
                        if (!hotPartitionIdsForSchedule.contains(parentId)) {
                            continue;
                        }
                        long parentRDDId = Long.parseLong(parentId.split(CriticalPathUtil.PARTITION_FLAG)[0]);
                        if (p.belongRDD.rddId.equals(parentRDDId)) {
                            needToConsiderOverStage = false;
                        }
                        if (stageIdToRDDIdSetForSchedule.get(stageId).contains(parentRDDId)) {
                            int curRC = hotPartitionRC.getOrDefault(parentId, 0);
                            hotPartitionRC.put(parentId, curRC + 1);
                        } else if(needToConsiderOverStage) {
                            RDD curParentRDD = rddMapForSchedule.get(parentRDDId);
                            for (int i = 0; i < curParentRDD.partitionNum; i++) {
                                String newParentId = String.format("%d_%d", parentRDDId, i);
                                int curRC = hotPartitionRC.getOrDefault(newParentId, 0);
                                hotPartitionRC.put(newParentId, curRC + 1);
                            }
                        }
                    }
                }
            }
        }
        // 3. 第三类rc 指向action的rc
        Map<Long, Integer> rddIdToActionNum = generateRDDIdToActionNum(jobList);
        for (Map.Entry<Long, Integer> entry : rddIdToActionNum.entrySet()) {
            long key = entry.getKey();
            if (!hotRDDIdsForSchedule.contains(key)) {
                continue;
            }
            int value = entry.getValue();
            RDD curRDD = rddMapForSchedule.get(key);
            for (int i = 0; i < curRDD.partitionNum; i++) {
                String partitionId = String.format("%d_%d", key, i);
                int curRC = hotPartitionRC.getOrDefault(partitionId, 0);
                hotPartitionRC.put(partitionId, curRC + value);
            }
        }
        return hotPartitionRC;
    }



    @Deprecated
    public static void updateHotDataRefCountByRDD(Map<Long, Integer> hotDataRC, RDD currentUsedRDD, Map<Long, Integer> rddIdToActionNum) { // fix bug: RDD 27's rc != 0
        // TODO: 已假设Stage中RDD不会重复出现，待验证
        Long rddId = currentUsedRDD.rddId;
        if(hotDataRC.containsKey(rddId) && rddIdToActionNum.containsKey(rddId) && rddIdToActionNum.get(rddId) > 0) {
            hotDataRC.put(rddId, hotDataRC.get(rddId) - 1);
            rddIdToActionNum.put(rddId, rddIdToActionNum.get(rddId) - 1);
        }
        for(long parentId : currentUsedRDD.rddParentIDs) {
            if(hotDataRC.containsKey(parentId)) {
                hotDataRC.put(parentId, hotDataRC.get(parentId) - 1);
            }
        }
    }

    public static void updateHotDataRefCountByStage(Map<Long, Integer> hotDataRC, Stage currentRunStage, Map<Long, Integer> rddIdToActionNum) { // fix bug: RDD 27's rc != 0
        // TODO: 已假设Stage中RDD不会重复出现，待验证
        for (RDD rdd : currentRunStage.rdds) {
            for(long parentId : rdd.rddParentIDs) {
                if(hotDataRC.containsKey(parentId)) {
                    hotDataRC.put(parentId, hotDataRC.get(parentId) - 1);
                }
            }
        }
        RDD lastRDD = SimpleUtil.lastRDDOfStage(currentRunStage);
        Long rddId = lastRDD.rddId;
        if(hotDataRC.containsKey(rddId) && rddIdToActionNum.containsKey(rddId)) { //  && rddIdToActionNum.get(rddId) > 0
            hotDataRC.put(rddId, hotDataRC.get(rddId) - 1);
            rddIdToActionNum.put(rddId, rddIdToActionNum.get(rddId) - 1);
        }
    }

    // TODO: partitionIdToActionNum有存在的必要吗？
    public static void updateHotPartitionRefCountByTask(Map<String, Integer> hotPartitionRC,
                                                        Task curRunTask,
                                                        Map<String, Integer> partitionIdToActionNum) {
        // 1. 更新第一类rc和第二类rc
        long stageId = curRunTask.stageId;
        for (Partition p : curRunTask.getPartitions()) {
            boolean needToConsiderOverStage = true;
            for (String parentId : p.getParentIds()) {
                if (!hotPartitionIdsForSchedule.contains(parentId)) {
                    continue;
                }
                long parentRDDId = Long.parseLong(parentId.split(CriticalPathUtil.PARTITION_FLAG)[0]);
                if (p.belongRDD.rddId.equals(parentRDDId)) {
                    needToConsiderOverStage = false;
                }
                if (stageIdToRDDIdSetForSchedule.get(stageId).contains(parentRDDId)) {
                    hotPartitionRC.put(parentId, hotPartitionRC.get(parentId) - 1);
                } else if(needToConsiderOverStage) {
                    RDD curParentRDD = rddMapForSchedule.get(parentRDDId);
                    for (int i = 0; i < curParentRDD.partitionNum; i++) {
                        String newParentId = String.format("%d_%d", parentRDDId, i);
                        hotPartitionRC.put(newParentId, hotPartitionRC.get(newParentId) - 1);
                    }
                }
            }
        }
        // 2. 更新第三类rc
        // 应该是job的last stage的last rdd
        if (!lastStageIdsOfJobsForSchedule.contains(curRunTask.stageId)) {
            return;
        }
        RDD lastRDD = SimpleUtil.lastRDDOfStage(stageMapForSchedule.get(curRunTask.stageId));
        if (!hotRDDIdsForSchedule.contains(lastRDD.rddId)) {
            return;
        }
        for (Partition p : curRunTask.getPartitions()) {
            if (p.belongRDD.rddId.equals(lastRDD.rddId)) {
                hotPartitionRC.put(p.getPartitionId(), hotPartitionRC.get(p.getPartitionId()) - 1);
                partitionIdToActionNum.put(p.getPartitionId(), partitionIdToActionNum.get(p.getPartitionId()) - 1);
            }
        }
    }

    public static Map<Long, Integer> generateRDDIdToActionNum(List<Job> jobList) {
        Map<Long, Integer> rddIdToActionNum = new HashMap<>();
        for(Job job : jobList) {
            long curLastRDDId = SimpleUtil.lastRDDOfStage(SimpleUtil.lastStageOfJob(job)).rddId;
            rddIdToActionNum.put(curLastRDDId, rddIdToActionNum.getOrDefault(curLastRDDId, 0) + 1);
        }
        return rddIdToActionNum;
    }

    public static Map<String, Integer> generatePartitionIdToActionNum(List<Job> jobList) {
        Map<String, Integer> partitionIdToActionNum = new HashMap<>();
        Map<Long, Integer> rddIdToActionNum = generateRDDIdToActionNum(jobList);
        for (Map.Entry<Long, Integer> entry : rddIdToActionNum.entrySet()) {
            long key = entry.getKey();
            if (!hotRDDIdsForSchedule.contains(key)) {
                continue;
            }
            int value = entry.getValue();
            RDD curRDD = rddMapForSchedule.get(key);
            for (int i = 0; i < curRDD.partitionNum; i++) {
                String partitionId = String.format("%d_%d", key, i);
                int curRC = partitionIdToActionNum.getOrDefault(partitionId, 0);
                partitionIdToActionNum.put(partitionId, curRC + value);
            }
        }
        return partitionIdToActionNum;
    }

}
