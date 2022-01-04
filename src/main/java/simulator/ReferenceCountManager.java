package simulator;

import entity.Job;
import entity.RDD;
import entity.event.JobStartEvent;
import utils.CacheSketcher;
import utils.SimpleUtil;
import java.util.*;

/**
 * 采用twice data的思路生成reference count
 */
public class ReferenceCountManager {

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

    public static Map<Long, Integer> generateRDDIdToActionNum(List<Job> jobList) {
        Map<Long, Integer> rddIdToActionNum = new HashMap<>();
        for(Job job : jobList) {
            long curLastRDDId = SimpleUtil.lastRDDOfStage(SimpleUtil.lastStageOfJob(job)).rddId;
            rddIdToActionNum.put(curLastRDDId, rddIdToActionNum.getOrDefault(curLastRDDId, 0) + 1);
        }
        return rddIdToActionNum;
    }

}
