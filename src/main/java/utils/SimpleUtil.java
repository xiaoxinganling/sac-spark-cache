package utils;

import entity.RDD;
import entity.Stage;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;

import java.util.*;

public class SimpleUtil {

    /**
     *
     * @param jobs
     * @return the number of stage in the provided job list
     */
    public static int stageNumOfJobs(List<JobStartEvent> jobs) {
        HashSet<Long> stageSet = new HashSet<>();
        int res = 0;
        for(JobStartEvent jse : jobs) {
            for(Stage stage : jse.stages) {
                assert(!stageSet.contains(stage.stageId));
                stageSet.add(stage.stageId);
            }
            res += jse.stages.size();
        }
        return res;
    }

    /**
     * 判断job是否包含并行的stage，即是否包含in_ref > 1的stage
     * @param job
     * @return
     */
    public static boolean jobContainsParallelStages(JobStartEvent job, List<StageCompletedEvent> stages) {
        Map<Long, Stage> stageMap = filteredStageMapOfJob(job, stages);
        for(Stage stage : job.stages) {
            if(stage.parentIDs.size() > 1) {
                int sizeInJob = 0;
                for(Long parentId : stage.parentIDs) {
                    if(stageMap.containsKey(parentId)) {
                        sizeInJob++;
                    }
                }
                if(sizeInJob > 1) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 获取某个具体job中需要缓存的rdd
     * @param allToBeCache
     * @param job
     * @param stages
     * @return
     */
    public static List<Long> rddToCacheInJob(List<Long> allToBeCache, JobStartEvent job, List<StageCompletedEvent> stages) {
        List<Long> toBeCache = new ArrayList<>();
        Set<Long> actualStages = new HashSet<>();
        Set<Long> actualRDDs = new HashSet<>();
        for(StageCompletedEvent sce : stages) {
            actualStages.add(sce.stage.stageId);
        }
        for(Stage stage : job.stages) {
            if(!actualStages.contains(stage.stageId)) {
                continue;
            }
            for(RDD rdd : stage.rdds) {
                actualRDDs.add(rdd.rddId);
            }
        }
        for(long rddId : allToBeCache) {
            if(actualRDDs.contains(rddId)) {
                toBeCache.add(rddId);
            }
        }
        return toBeCache;
    }

    public static List<Long> rddToCacheInApplication(List<JobStartEvent> jobs, List<StageCompletedEvent> stages) {
        int[][] simpleDAG = CacheSketcher.generateSimpleDAGByJobsAndStages(jobs, stages);
        List<Long> toBeCache = new ArrayList<>();
        for(int i = 0; i < simpleDAG.length - jobs.size(); i++) {
            int sum = 0;
            for(int j = 0; j < simpleDAG[0].length; j++) {
                if(simpleDAG[i][j] > 0) {
                    sum += 1;
                }
            }
            if(sum > 1) {
                toBeCache.add((long) i);
            }
        }
        return toBeCache;
    }

    /**
     * job包含的stage组成的map集合
     * @param job
     * @return
     */
    public static Map<Long, Stage> stageMapOfJob(JobStartEvent job) {
        Map<Long, Stage> stageMap = new HashMap<>();
        for(Stage s : job.stages) {
            assert !stageMap.containsKey(s.stageId);
            stageMap.put(s.stageId, s);
        }
        return stageMap;
    }

    /**
     * job包含的stage中实际被执行的stage组成的map集合
     * @param job
     * @param stages
     * @return
     */
    public static Map<Long, Stage> filteredStageMapOfJob(JobStartEvent job, List<StageCompletedEvent> stages) {
        Set<Long> actualStages = new HashSet<>();
        for(StageCompletedEvent sce : stages) {
            actualStages.add(sce.stage.stageId);
        }
        Map<Long, Stage> stageMap = new HashMap<>();
        for(Stage s : job.stages) {
            assert !stageMap.containsKey(s.stageId);
            if(actualStages.contains(s.stageId)) {
                stageMap.put(s.stageId, s);
            }
        }
        return stageMap;
    }

    /**
     *
     * @param job
     * @return 返回job的最后一个stage，即out ref为0的stage
     */
    public static Stage lastStageOfJob(JobStartEvent job) {
        Map<Long, Stage> stageMap = stageMapOfJob(job);
        Map<Long, Integer> stageOutRef = new HashMap<>();
        for(Stage s : job.stages) {
            for(Long parentId : s.parentIDs) {
                if(stageMap.containsKey(parentId)) {
                    stageOutRef.put(parentId, stageOutRef.getOrDefault(parentId, 0) + 1);
                }
            }
        }
        int lastSize = 0;
        Stage resultStage = null;
        for(Stage s : job.stages) {
            if(stageOutRef.getOrDefault(s.stageId, 0) == 0) {
                lastSize++;
                resultStage = s;
            }
        }
        assert lastSize == 1;
        return resultStage;
    }

    /**
     *
     * @param stage
     * @param stageMap
     * @return 计算以该stage结尾的stage序列的时间
     */
    public static int computeTimeOfStage(Stage stage, Map<Long, Stage> stageMap) {
        if(stage == null || !stageMap.containsKey(stage.stageId)) {
            return 0;
        }
        int ownTime = stage.rdds.size();
        int max = 0;
        for(Long parentId : stage.parentIDs) {
            max = Math.max(max, computeTimeOfStage(stageMap.get(parentId), stageMap));
        }
        return ownTime + max;
    }


    /**
     * 计算该Stage的时间+所有parent的时间
     * @param stage
     * @param stageMap
     * @return
     */
    public static int computeTimeOfStageWithAccumulation(Stage stage, Map<Long, Stage> stageMap) {
        if(stage == null || !stageMap.containsKey(stage.stageId)) {
            return 0;
        }
        int ownTime = stage.rdds.size();
        int parentTime = 0;
        for(Long parentId : stage.parentIDs) {
            parentTime += computeTimeOfStageWithAccumulation(stageMap.get(parentId), stageMap);
        }
        return ownTime + parentTime;
    }

    /**
     *
     * @param stage
     * @param stageMap
     * @return 计算以该stage结尾的stage序列的时间，每次都选择计算时间最短的parent
     */
    public static int computeTimeOfStageWithShortestPath(Stage stage, Map<Long, Stage> stageMap) {
        if(stage == null || !stageMap.containsKey(stage.stageId)) {
            return 0;
        }
        int ownTime = stage.rdds.size();
        int min = Integer.MAX_VALUE;
        for(Long parentId : stage.parentIDs) {
            min = Math.min(min, computeTimeOfStageWithShortestPath(stageMap.get(parentId), stageMap));
        }
        return min == Integer.MAX_VALUE ? ownTime : ownTime + min;
    }

    /**
     *
     * @param choseRDD
     * @param stage
     * @param stageMap
     * @return 计算以该stage结尾的stage序列在缓存了choseRDD的情况下的时间
     */
    public static int computeTimeOfStageWithCache(Set<Long> choseRDD, Stage stage, Map<Long, Stage> stageMap) {
        if(stage == null || !stageMap.containsKey(stage.stageId)) {
            return 0;
        }
        // KEYPOINT: 一般偏向于rdd的出现顺序与rdd id的大小一致
        int ownTime = stage.rdds.size();
        stage.rdds.sort((o1, o2) -> (int) (o1.rddId - o2.rddId));
        for(int i = stage.rdds.size() - 1; i >= 0; i--) {
            if(choseRDD.contains(stage.rdds.get(i).rddId)) {
                ownTime -= (i + 1);
                break;
            }
        }
        int max = 0;
        for(Long parentId : stage.parentIDs) {
            max = Math.max(computeTimeOfStageWithCache(choseRDD, stageMap.get(parentId), stageMap), max);
        }
        return ownTime + max;
    }

    /**
     * 计算以该stage结尾的stage序列在缓存了choseRDD的情况下的时间，所有stage的时间采用累加的方式
     * @param choseRDD
     * @param stage
     * @param stageMap
     * @return
     */
    public static int computeTimeOfStageWithCacheAndAccumulation(Set<Long> choseRDD, Stage stage, Map<Long, Stage> stageMap) {
        if(stage == null || !stageMap.containsKey(stage.stageId)) {
            return 0;
        }
        // KEYPOINT: 一般偏向于rdd的出现顺序与rdd id的大小一致
        int ownTime = stage.rdds.size();
        stage.rdds.sort((o1, o2) -> (int) (o1.rddId - o2.rddId));
        for(int i = stage.rdds.size() - 1; i >= 0; i--) {
            if(choseRDD.contains(stage.rdds.get(i).rddId)) {
                ownTime -= (i + 1);
                break;
            }
        }
        int parentTime = 0;
        for(Long parentId : stage.parentIDs) {
            parentTime += computeTimeOfStageWithCacheAndAccumulation(choseRDD, stageMap.get(parentId), stageMap);
        }
        return ownTime + parentTime;
    }

    /**
     *
     * @param choseRDD
     * @param stage
     * @param stageMap
     * @param isLong: true/false表示选择原有计算时间最长/短的parent
     * @return 计算以该stage结尾的stage序列在缓存了choseRDD的情况下的时间, 每次选择原有计算时间(最长/最短)的parent
     */
    public static int computeTimeOfStageWithCacheByLSPath(Set<Long> choseRDD, Stage stage, Map<Long, Stage> stageMap, boolean isLong) {
        // KEYPOINT: 一般偏向于rdd的出现顺序与rdd id的大小一致
        if(stage == null || !stageMap.containsKey(stage.stageId)) {
            return 0;
        }
        int ownTime = stage.rdds.size();
        stage.rdds.sort((o1, o2) -> (int) (o1.rddId - o2.rddId));
        for(int i = stage.rdds.size() - 1; i >= 0; i--) {
            if(choseRDD.contains(stage.rdds.get(i).rddId)) {
                ownTime -= (i + 1);
                break;
            }
        }
        long maxOrMinId = -1;
        int maxOrMinTime = isLong ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        for(Long parentId : stage.parentIDs) {
            Stage parentStage = stageMap.get(parentId);
            if((isLong && parentStage.rdds.size() > maxOrMinTime) ||
                    (!isLong && parentStage.rdds.size() < maxOrMinTime)) {
                maxOrMinId = parentId;
                maxOrMinTime = parentStage.rdds.size();
            }
        }
        return maxOrMinId == -1 ? ownTime : ownTime +
                computeTimeOfStageWithCacheByLSPath(choseRDD, stageMap.get(maxOrMinId), stageMap, isLong);
    }

    /**
     * 返回newValue相较于source的差异，例如source=2，newValue=3，差异=(3-2)/2=0.5
     * @param source
     * @param newValue
     * @return
     */
    public static double generateDifferenceRatio(long source, long newValue) {
        if(source == 0) {
            if(newValue == 0) {
                return 0;
            }else{
                return Double.MAX_VALUE;//TODO: need to review
            }
        }
        return Math.abs(source - newValue) / (double) source;
    }

}
