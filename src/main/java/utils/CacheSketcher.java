package utils;

import entity.RDD;
import entity.Stage;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class CacheSketcher {

    public static Map<Long, Integer> generateRefPerStage(Stage stage) {
        Map<Long, Integer> res = new HashMap<>();
        Map<Long, RDD> rddMap = new HashMap<>();
        for(RDD rdd : stage.rdds) {
            rddMap.put(rdd.rddId, rdd);
        }
        for(RDD rdd : stage.rdds) {
            for(Long parent : rdd.rddParentIDs) {
                res.put(parent, res.getOrDefault(parent, 0) + 1);
            }
        }
        for (Map.Entry<Long, Integer> entry : res.entrySet()) {
            Integer curValue = entry.getValue();
            Queue<Long> keyQueue = new LinkedList<>();
            keyQueue.offer(entry.getKey());
            while (!keyQueue.isEmpty()) {
                Long curKey = keyQueue.poll();
                for (Long parent : rddMap.get(curKey).rddParentIDs) {
                    keyQueue.offer(parent);
                    res.put(parent, res.getOrDefault(parent, 0) + curValue - 1);
                }
            }
        }
        return res;
    }

    // KEYPOINT: 最终需要考虑跨job的ref
    public static Map<Long, Integer> generateRefPerJob(JobStartEvent job) {
        Map<Long, Integer> res = generateRDDDirectRefPerJob(job, null);
        Map<Long, RDD> rddMap = generateRDDMapPerJob(job);
        for (Map.Entry<Long, Integer> entry : res.entrySet()) {
            Integer curValue = entry.getValue();
            if(curValue <= 1) {
                continue;
            }
            Queue<Long> keyQueue = new LinkedList<>();
            keyQueue.offer(entry.getKey());
            while (!keyQueue.isEmpty()) {
                Long curKey = keyQueue.poll();
                for (Long parent : rddMap.get(curKey).rddParentIDs) {
                    keyQueue.offer(parent);
                    res.put(parent, res.getOrDefault(parent, 0) + curValue - 1);
                }
            }
        }
        return res;
    }

    // FIXME: 跨job,简单累加
    // FIXME: 触发action的job ref为0
    // KEYPOINT a
    public static Map<Long, Integer> generateRefForJobs(List<JobStartEvent> jobs) {
        Map<Long, Integer> res = new HashMap<>();
        for(JobStartEvent job : jobs) {
            for(Map.Entry<Long, Integer> entry : generateRefPerJob(job).entrySet()) {
                int value = Math.max(entry.getValue(), 1);
                res.put(entry.getKey(), res.getOrDefault(entry.getKey(), 0) + value);
            }
        }
        return res;
    }

    public static Map<Long, Long> generatePartitionPerStage(Stage stage) {
        Map<Long, Long> res = new HashMap<>();
        for(RDD rdd : stage.rdds) {
            assert !res.containsKey(rdd.rddId) || (rdd.partitionNum.equals(res.get(rdd.rddId)));
            res.put(rdd.rddId, rdd.partitionNum);
        }
        return res;
    }

    /*
    KEYPOINT: 是否需要调用generatePartitionPerStage? —— 不需要
     */
    public static Map<Long, Long> generatePartitionPerJob(JobStartEvent job) {
        Map<Long, Long> res = new HashMap<>();
        for(Stage stage : job.stages) {
            for(RDD rdd : stage.rdds) {
                assert !res.containsKey(rdd.rddId) || (rdd.partitionNum.equals(res.get(rdd.rddId)));
                res.put(rdd.rddId, rdd.partitionNum);
            }
        }
        return res;
    }

    // KEYPOINT c
    public static Map<Long, Long> generatePartitionForJobs(List<JobStartEvent> jobs) {
        assert jobs.size() > 0;
        Map<Long, Long> rddPartition = new HashMap<>();
        for(JobStartEvent job : jobs) {
            for(Map.Entry<Long, Long> entry : generatePartitionPerJob(job).entrySet()) {
                if(rddPartition.containsKey(entry.getKey())) {
                    assert rddPartition.get(entry.getKey()).equals(entry.getValue());
                }else{
                    rddPartition.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return rddPartition;
    }

    /*
    TODO: calculating time
     */
    public static Map<Long, Long> generateComputingTimePerStage(Stage stage) {
        return null;
    }

    /*
    TODO: calculating space
     */
    public static Map<Long, Long> generateSpacePerStage(Stage stage) {
        return null;
    }

    public static Map<Long, RDD> generateRDDMapPerJob(JobStartEvent job) {
        Map<Long, RDD> idToRDD = new HashMap<>();
        for(Stage stage : job.stages) {
            for(RDD rdd : stage.rdds) {
                // TODO: 不同job可能存在重复rdd
                idToRDD.put(rdd.rddId, rdd);
            }
        }
        return idToRDD;
    }

    // FIXME: 触发Job的RDD的direct ref为0

    /**
     * 返回每個job的direct ref，actualStages are stages actually used in this job
     * @param job
     * @param actualStages
     * @return
     */
    public static Map<Long, Integer> generateRDDDirectRefPerJob(JobStartEvent job, Set<Long> actualStages) {
        Map<Long, Integer> rddToDirectRef = new HashMap<>();
        int curStageSize = 0;
        for(Stage stage : job.stages) {
            if(actualStages != null && !actualStages.contains(stage.stageId)) {
                continue;
            }
//            System.out.println("stage: -> " + curStageSize++ + " / " + actualStages.size());
            for (RDD rdd : stage.rdds) {
                // TODO: 不同job可能存在重复rdd
                if(!rddToDirectRef.containsKey(rdd.rddId)) {
                    // 防止漏掉没有出边的rdd
                    rddToDirectRef.put(rdd.rddId, 0);
                }
                for(long parent : rdd.rddParentIDs) {
                    rddToDirectRef.put(parent, rddToDirectRef.getOrDefault(parent, 0) + 1);
                }
            }
        }
        return rddToDirectRef;
    }

    public static Map<Long, Integer> generateRDDDirectRefForJobs(List<JobStartEvent> jobs, Set<Long> actualStages) {
        Map<Long, Integer> res = new HashMap<>();
        int curJobSize = 0;
        for(JobStartEvent job : jobs) {
            for(Map.Entry<Long, Integer> entry : generateRDDDirectRefPerJob(job, actualStages).entrySet()) {
                if(entry.getValue().equals(0)) { //  add action to this RDD
                    entry.setValue(1);
                }
                res.put(entry.getKey(), res.getOrDefault(entry.getKey(), 0) + entry.getValue());
            }
            // System.out.println("job: -> " + ++curJobSize + " / " + jobs.size());
        }
        return res;
    }

    // KEYPOINT: MERGE DAG => if ok => pure_ref > 2; if not => pure_ref > 2 + others
    /**
     *
     * @param jobs
     * @param stages
     * if only need jobs, pass the variable `stages` with size=0 or null
     * @return DAG represented as array[][]
     */
    public static int[][] generateSimpleDAGByJobsAndStages(List<JobStartEvent> jobs, List<StageCompletedEvent> stages) {
        // judge the size of rdd
        // KEYPOINT: maintain a map from `id of job list` to `id of array` ×
        // KEYPOINT: modify len(array) to max value of rdd's id
        long max = Long.MIN_VALUE;
        for(JobStartEvent jse : jobs) {
            for(Stage stage : jse.stages) {
                for(RDD rdd : stage.rdds) {
                    max = Math.max(max, rdd.rddId);
                }
            }
        }
        int rddSize = (max == Long.MIN_VALUE) ? 0 : (int) max + 1;
        // KEYPOINT: use stage list to accelerate
        Set<Long> actuallyRunStage = new HashSet<>();
        if(stages != null && stages.size() > 0) {
            for(StageCompletedEvent sce : stages) {
                actuallyRunStage.add(sce.stage.stageId);
            }
        }
        // judge the size of action
        int actionSize = jobs.size();
        // res array
        int[][] resArray = new int[rddSize + actionSize][rddSize + actionSize];
        for(int i = 0; i < jobs.size(); i++) {
            Set<Long> curJobRDDSet = new HashSet<>();
            Set<Long> curAddedRDDSet = new HashSet<>();
            for(Stage stage : jobs.get(i).stages) { // FIXME: filter stage
                if(stages != null && stages.size() > 0 && !actuallyRunStage.contains(stage.stageId)) {
                    continue;
                }
                for(RDD rdd : stage.rdds) {
                    curJobRDDSet.add(rdd.rddId);
                    if(rdd.rddParentIDs.size() == 0){
                        continue;
                    }
                    for(long sourceId : rdd.rddParentIDs) {
                        resArray[(int)sourceId][rdd.rddId.intValue()]++;
                        curAddedRDDSet.add(sourceId);
                    }
                }
            }
            List<Long> lastRDDIds = new ArrayList<>();
            for(long rddId : curJobRDDSet) {
                if(!curAddedRDDSet.contains(rddId)) {
                    lastRDDIds.add(rddId);
                }
            }
            // FIXME: better performance
            // KEYPOINT: performance is ok
            // System.out.println("rdd to call action: " + lastRDDIds);
            // FIXME: assume that every job is useful, which contains at least one RDD
            assert lastRDDIds.size() == 1;
            resArray[lastRDDIds.get(0).intValue()][rddSize + i]++;
        }
        return resArray;
    }

    public static int generateStageHitNumWithStagesV0(List<StageCompletedEvent> stages, List<Long> choseRDD) throws IOException {
        int res = 0;
        for (StageCompletedEvent sce : stages) {
            int coverRDDOfStage = rddsAreUsefulToStage(sce.stage, choseRDD);
            if (coverRDDOfStage > 0) {
                res++;
            }
        }
        return res;
    }

    /**
     * KEYPOINT TODO: split by shuffle RDD info
     * @param stages
     * @param choseRDD
     * @return hit number of stage according to different chose RDD and actually run stages
     */
    public static int generateStageHitNumWithStagesV1(List<StageCompletedEvent> stages, List<Long> choseRDD) throws IOException {
        int res = 0;
        // FIXME: it's only for SVM and TeraSort's `cover 2 more RDD` situation
        Set<Long> noShuffleRDD = new HashSet<>();
        long maxRDDId = Long.MIN_VALUE;
        for(long l : choseRDD) {
            maxRDDId = Math.max(l, maxRDDId);
        }
        UnionFindUtil uf = new UnionFindUtil((int) (maxRDDId + 1));
        // end FIXME: it's only for SVM and TeraSort's `cover 2 more RDD` situation
        for(StageCompletedEvent sce : stages) {
            int coverRDDOfStage = rddsAreUsefulToStage(sce.stage, choseRDD);
            List<Long> noShuffleRDDEveryStage = new ArrayList<>();
            if(coverRDDOfStage > 0) {
                res++;
                // FIXME: it's only for SVM and TeraSort's `cover 2 more RDD` situation
                if (coverRDDOfStage > 1) {
                    System.out.println("stage: " + sce.stage.stageId + ", cover RDD num: " + coverRDDOfStage);
                    for(RDD rdd : sce.stage.rdds) {
                        if(choseRDD.contains(rdd.rddId)) {
//                            System.out.print(rdd.rddId + ", ");
                            noShuffleRDD.add(rdd.rddId);
                            assert(!noShuffleRDDEveryStage.contains(rdd.rddId));
                            noShuffleRDDEveryStage.add(rdd.rddId);
                        }
                    }
                    //System.out.println(" <--RDD items");
                    Collections.sort(noShuffleRDDEveryStage);
                    // deal with list
                    for(int i = 0; i < noShuffleRDDEveryStage.size() - 1; i++) {
                        uf.union(noShuffleRDDEveryStage.get(i).intValue(), noShuffleRDDEveryStage.get(i + 1).intValue());
                    }
                }
                // end FIXME: it's only for SVM and TeraSort's `cover 2 more RDD` situation
            }
            else{
                // FIXME: it's only for SVM and TeraSort's uncovered Stage
                System.out.println(sce.stage.stageId + ",");
            }
        }
//        System.out.println(noShuffleRDD + ", size: " + noShuffleRDD.size() + "/" + choseRDD.size()); // FIXME: it's only for SVM and TeraSort's `cover 2 more RDD` situation
        // FIXME: it's only for SVM and TeraSort's `cover 2 more RDD` situation
//        int totalConditionNum = choseRDD.size() == 1 ? 1 : choseRDD.size() * (choseRDD.size() - 1) / 2;
        long totalConditionNum = NumberUtil.comb(choseRDD.size(), 2);
//        if(totalConditionNum != NumberUtil.comb(choseRDD.size(), 2)) {
//            System.out.println(choseRDD.size() + " choose 2: " + totalConditionNum + " " + NumberUtil.comb(choseRDD.size(), 2));
//        }
        int connectedConditionNum = 0;
        long totalConditionNumThree = NumberUtil.comb(choseRDD.size(), 3);
//        if(totalConditionNumThree != NumberUtil.comb(choseRDD.size(), 3)) {
//            System.out.println(choseRDD.size() + " choose 3: " + totalConditionNumThree + " " + NumberUtil.comb(choseRDD.size(), 3));
//        }
        int connectedConditionNumThree = 0;
        long totalConditionNumFour = NumberUtil.comb(choseRDD.size(), 4);
//       if(totalConditionNumFour != NumberUtil.comb(choseRDD.size(), 4)) {
//            System.out.println(choseRDD.size() + " choose 4: " + totalConditionNumFour + " " + NumberUtil.comb(choseRDD.size(), 4));
//       }
        int connectedConditionNumFour = 0;
        for(int i = 0; i < choseRDD.size(); i++) {
            for(int j = i + 1; j < choseRDD.size(); j++) {
                if(uf.connected(choseRDD.get(i).intValue(), choseRDD.get(j).intValue())) {
                    connectedConditionNum++;
                }
                for(int k = j + 1; k < choseRDD.size(); k++) {
                    // i <-> j or j <-> k or i <-> k
                    int iValue = choseRDD.get(i).intValue();
                    int jValue = choseRDD.get(j).intValue();
                    int kValue = choseRDD.get(k).intValue();
                    if(uf.connected(iValue, jValue) || uf.connected(jValue, kValue)
                            || uf.connected(iValue, kValue)) {
                        connectedConditionNumThree++;
                    }
                    for(int m = k + 1; m < choseRDD.size(); m++) {
                        int mValue = choseRDD.get(m).intValue();
                        // i <-> j or i <-> m or i <-> k
                        // j <-> k or j <-> m or k <-> m
                        if(uf.connected(iValue, jValue) || uf.connected(iValue, mValue) ||
                                uf.connected(iValue, kValue) || uf.connected(jValue, kValue) ||
                                uf.connected(jValue, mValue) || uf.connected(kValue, mValue)) {
                            connectedConditionNumFour++;
                        }
                    }
                }
            }
        }
        System.out.println("size: " + noShuffleRDD.size() + "/" + choseRDD.size());
        System.out.println("connected condition ratio: " + connectedConditionNum + "/" + totalConditionNum +
                " = " + connectedConditionNum / (double) totalConditionNum);
        System.out.println("connected condition three ratio: " + connectedConditionNumThree + "/" + totalConditionNumThree +
                " = " + connectedConditionNumThree / (double) totalConditionNumThree);
        System.out.println("connected condition for four ratio: " + connectedConditionNumFour + "/" + totalConditionNumFour +
                " = " + connectedConditionNumFour / (double) totalConditionNumFour);
        BufferedWriter bw = new BufferedWriter(new FileWriter("no_shuffle_ratio", true));
//        bw.write(connectedConditionNum + "/" + totalConditionNum +
//                " " + connectedConditionNum / (double) totalConditionNum + "\n");
//        bw.write(connectedConditionNum / (double) totalConditionNum + ", \n");
//        bw.write(connectedConditionNumThree / (double) totalConditionNumThree + ", \n");
        bw.write(connectedConditionNumFour / (double) totalConditionNumFour + ", \n");
//        bw.write(connectedConditionNum  / (double) totalConditionNum +
//                " " + connectedConditionNumThree / (double) totalConditionNumThree +
//                " " + connectedConditionNumFour / (double) totalConditionNumFour + ", \n");
        bw.close();
        // end FIXME: it's only for SVM and TeraSort's `cover 2 more RDD` situation
        return res;
    }

    public static double generateStageHitRatioWithJobs(List<JobStartEvent> jobs, List<Long> choseRDD) {
        int res = 0;
        double totalStageNum = 0;
        for(JobStartEvent job : jobs) {
            for(Stage stage : job.stages) {
                if(rddsAreUsefulToStage(stage, choseRDD) > 0) {
                    res++;
                }
                totalStageNum++;
            }
        }
        return res / totalStageNum;
    }

    /**
     *
     * @param stage
     * @param choseRDD
     * @return the num of rdd chose in one stage
     */
    public static int rddsAreUsefulToStage(Stage stage, List<Long> choseRDD) {
        int sum = 0;
        for(RDD rdd : stage.rdds) {
            if(choseRDD.contains(rdd.rddId)) {
                sum++;
            }
        }
        return sum;
    }

    // KEYPOINT: 去重
    public static Set<Long> generate0IndegreeRDDPerJob(JobStartEvent job) {
        Set<Long> zeroIndegreeRDD = new HashSet<>();
        for(Stage stage : job.stages) {
            for (RDD rdd : stage.rdds) {
                // TODO: 不同job可能存在重复rdd
                if (rdd.rddParentIDs.size() == 0) {
                    zeroIndegreeRDD.add(rdd.rddId);
                }
            }
        }
        return zeroIndegreeRDD;
    }

    // KEYPOINT: 去重
    public static Set<Long> generate0OutDegreeRDDFromDirectRef(Map<Long, Integer> rddToDirectRef) {
        Set<Long> zeroOutdegreeRDD = new HashSet<>();
        for(Map.Entry<Long, Integer> entry : rddToDirectRef.entrySet()) {
            if(entry.getValue() == 0) {
                zeroOutdegreeRDD.add(entry.getKey());
            }
        }
        return zeroOutdegreeRDD;
    }

    // KEYPOINT
    public static Map<Long, Integer> generateHopComputeTimePerJob(JobStartEvent job) {
        Set<Long> zeroIndegreeRDD = generate0IndegreeRDDPerJob(job);
        Map<Long, Integer> rddToHop = generateHopPerJob(job);
        int maxIndegreeRDDHopValue = Integer.MIN_VALUE;
        for(long i : zeroIndegreeRDD) {
            maxIndegreeRDDHopValue = Math.max(maxIndegreeRDDHopValue, rddToHop.get(i));
        }
        assert maxIndegreeRDDHopValue != Integer.MIN_VALUE;
        for(Map.Entry<Long, Integer> entry : rddToHop.entrySet()) {
            // KEYPOINT: 处理小于0的情况
            int toPut = Math.max(maxIndegreeRDDHopValue - rddToHop.get(entry.getKey()), 0);
            rddToHop.put(entry.getKey(), toPut);
        }
        return rddToHop;
    }

    // KEYPOINT b
    public static Map<Long, Integer> generateHopComputeTimeForJobs(List<JobStartEvent> jobs) {
        assert jobs.size() > 0;
        Map<Long, Integer> rddToHopComputeTime = new HashMap<>();
        for(JobStartEvent job : jobs) {
            for(Map.Entry<Long, Integer> entry : CacheSketcher.generateHopComputeTimePerJob(job).entrySet()) {
                if(rddToHopComputeTime.containsKey(entry.getKey())) {
                    assert rddToHopComputeTime.get(entry.getKey()).equals(entry.getValue());
                }else{
                    rddToHopComputeTime.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return rddToHopComputeTime;
    }

    // TODO: 是否能够直接用rdd id来代替呢? => 用Hop来代替 => 用RDD id来代替
    // KEYPOINT: 计算hop
    public static Map<Long, Integer> generateHopPerJob(JobStartEvent job) {
        // 这里会包含省略的Stage
        Map<Long, Integer> res = new HashMap<>();
        Map<Long, Integer> rddToDirectRef = generateRDDDirectRefPerJob(job, null);
        Map<Long, RDD> idToRDD = generateRDDMapPerJob(job);
        // List<Long> zeroIndegreeRDD = generate0IndegreeRDDPerJob(job);
        Set<Long> zeroOutdegreeRDD = generate0OutDegreeRDDFromDirectRef(rddToDirectRef);
        // TODO: need to assert
        //assert(zeroOutdegreeRDD.size() == 1);
        Queue<Long> bfsQueue = new LinkedList<>(zeroOutdegreeRDD);
        int depth = 1;
        while(!bfsQueue.isEmpty()) {
            int size = bfsQueue.size();
            for(int i = 0; i < size; i++) {
                long cur = bfsQueue.poll();
                res.put(cur, depth); // 蕴含的取max
                // TODO: 需要去重
                bfsQueue.addAll(idToRDD.get(cur).rddParentIDs);
            }
            depth++;
        }
        return res;
    }

    // (ref - 1) * time / space
    // KEYPOINT d
    public static Map<Long, Float> generateCostEffectiveness(List<JobStartEvent> jobs, List<StageCompletedEvent> stages) {
        Map<Long, Integer> refForAllJobs = generateRefForJobs(jobs);
        //Map<Long, Integer> directRefForAllJobs = generateRDDDirectRefForJobs(jobs);
        Map<Long, Integer> hopComputeTimeForAllJobs = generateHopComputeTimeForJobs(jobs);
        Map<Long, Long> partitionForAllJobs = generatePartitionForJobs(jobs);
        assert refForAllJobs.size() == hopComputeTimeForAllJobs.size();
        assert hopComputeTimeForAllJobs.size() == partitionForAllJobs.size();
        Map<Long, Float> costEffectiveness = new HashMap<>();
        for(long rddId : refForAllJobs.keySet()) {
            costEffectiveness.put(rddId, (refForAllJobs.get(rddId) - 1) * hopComputeTimeForAllJobs.get(rddId)
                    / (float) (partitionForAllJobs.get(rddId)));
        }
        return costEffectiveness;
    }

    // (ref - 1) * ln(time / space)
    // KEYPOINT e
    // FIXME: plus one for `ln`
    public static Map<Long, Float> generateLogCostEffectiveness(List<JobStartEvent> jobs, List<StageCompletedEvent> stages) {
        Map<Long, Integer> refForAllJobs = generateRefForJobs(jobs);
        //Map<Long, Integer> directRefForAllJobs = generateRDDDirectRefForJobs(jobs);
        Map<Long, Integer> hopComputeTimeForAllJobs = generateHopComputeTimeForJobs(jobs);
        Map<Long, Long> partitionForAllJobs = generatePartitionForJobs(jobs);
        assert refForAllJobs.size() == hopComputeTimeForAllJobs.size();
        assert hopComputeTimeForAllJobs.size() == partitionForAllJobs.size();
        Map<Long, Float> logCostEffectiveness = new HashMap<>();
        for(long rddId : refForAllJobs.keySet()) {
//            float value = Math.max(0, (refForAllJobs.get(rddId) - 1) * (float) Math.log(hopComputeTimeForAllJobs.get(rddId)
//                    / (float) (partitionForAllJobs.get(rddId))));
            float value = (refForAllJobs.get(rddId) - 1) * (float) Math.log(hopComputeTimeForAllJobs.get(rddId)
                    / (float) (partitionForAllJobs.get(rddId)) + 1);
            logCostEffectiveness.put(rddId, value);
        }
        return logCostEffectiveness;
    }

    public static Map<Long, Float> generateDirectCostEffectiveness(List<JobStartEvent> jobs, List<StageCompletedEvent> stages) {
        Map<Long, Integer> directRefForAllJobs = generateRDDDirectRefForJobs(jobs, null);
        Map<Long, Integer> hopComputeTimeForAllJobs = generateHopComputeTimeForJobs(jobs);
        Map<Long, Long> partitionForAllJobs = generatePartitionForJobs(jobs);
        assert directRefForAllJobs.size() == hopComputeTimeForAllJobs.size();
        assert hopComputeTimeForAllJobs.size() == partitionForAllJobs.size();
        Map<Long, Float> directCostEffectiveness = new HashMap<>();
        for(long rddId : directRefForAllJobs.keySet()) {
            directCostEffectiveness.put(rddId, (directRefForAllJobs.get(rddId) - 1) * hopComputeTimeForAllJobs.get(rddId)
                    / (float) (partitionForAllJobs.get(rddId)));
        }
        return directCostEffectiveness;
    }

    public static Map<Long, Float> generateLogDirectCostEffectiveness(List<JobStartEvent> jobs, List<StageCompletedEvent> stages) {
        Map<Long, Integer> directRefForAllJobs = generateRDDDirectRefForJobs(jobs, null);
        Map<Long, Integer> hopComputeTimeForAllJobs = generateHopComputeTimeForJobs(jobs);
        Map<Long, Long> partitionForAllJobs = generatePartitionForJobs(jobs);
        assert directRefForAllJobs.size() == hopComputeTimeForAllJobs.size();
        assert hopComputeTimeForAllJobs.size() == partitionForAllJobs.size();
        Map<Long, Float> logDirectCostEffectiveness = new HashMap<>();
        for(long rddId : directRefForAllJobs.keySet()) {
//            float value = Math.max(0, (directRefForAllJobs.get(rddId) - 1) * (float) Math.log(hopComputeTimeForAllJobs.get(rddId)
//                    / (float) (partitionForAllJobs.get(rddId))));
            float value = (directRefForAllJobs.get(rddId) - 1) * (float) Math.log(hopComputeTimeForAllJobs.get(rddId)
                    / (float) (partitionForAllJobs.get(rddId)) + 1);
            logDirectCostEffectiveness.put(rddId, value);
        }
        return logDirectCostEffectiveness;
    }

}
