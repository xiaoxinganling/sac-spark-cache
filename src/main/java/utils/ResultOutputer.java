package utils;

import entity.RDD;
import entity.Stage;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class ResultOutputer {

    // record every job's sum
    private static long[] sum = new long[12];

    private static double[] ALL_REPRESENT = new double[2];

    private static final String doublePrintFormat = "%.4f";

    @Deprecated
    public static void writeFullSketchStatistics(List<JobStartEvent> jobs, String fileName, List<StageCompletedEvent> stages) throws Exception{
        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName)); // "ref_time_space_ce"
        Map<Long, Integer> refForAllJobs = CacheSketcher.generateRefForJobs(jobs); // 可以不要
        Map<Long, Integer> directRefForAllJobs = CacheSketcher.generateRDDDirectRefForJobs(jobs, null);
        Map<Long, Integer> hopComputeTimeForAllJobs = CacheSketcher.generateHopComputeTimeForJobs(jobs); //可以是rdd id
        Map<Long, Long> partitionForAllJobs = CacheSketcher.generatePartitionForJobs(jobs);
        Map<Long, Float> costEffectiveness = new HashMap<>();
        Map<Long, Float> logCostEffectiveness = new HashMap<>();
        Map<Long, Float> directCostEffectiveness = new HashMap<>();
        Map<Long, Float> logDirectCostEffectiveness = new HashMap<>();
        for(long rddId : refForAllJobs.keySet()) {
            costEffectiveness.put(rddId, (refForAllJobs.get(rddId) - 1) * hopComputeTimeForAllJobs.get(rddId)
                    / (float) (partitionForAllJobs.get(rddId)));
        }
        for(long rddId : refForAllJobs.keySet()) {
//            float value = Math.max(0, (refForAllJobs.get(rddId) - 1) * (float) Math.log(hopComputeTimeForAllJobs.get(rddId)
//                    / (float) (partitionForAllJobs.get(rddId))));
            float value = (refForAllJobs.get(rddId) - 1) * (float) Math.log(hopComputeTimeForAllJobs.get(rddId)
                    / (float) (partitionForAllJobs.get(rddId)) + 1);
            logCostEffectiveness.put(rddId, value);
        }

        for(long rddId : directRefForAllJobs.keySet()) {
            directCostEffectiveness.put(rddId, (directRefForAllJobs.get(rddId) - 1) * hopComputeTimeForAllJobs.get(rddId)
                    / (float) (partitionForAllJobs.get(rddId)));
        }
        for(long rddId : directRefForAllJobs.keySet()) {
//            float value = Math.max(0, (directRefForAllJobs.get(rddId) - 1) * (float) Math.log(hopComputeTimeForAllJobs.get(rddId)
//                    / (float) (partitionForAllJobs.get(rddId))));
            float value = (directRefForAllJobs.get(rddId) - 1) * (float) Math.log(hopComputeTimeForAllJobs.get(rddId)
                    / (float) (partitionForAllJobs.get(rddId)) + 1);
            logDirectCostEffectiveness.put(rddId, value);
        }
        writeHashMap(bw, refForAllJobs);
        writeHashMap(bw, directRefForAllJobs);
        writeHashMap(bw, hopComputeTimeForAllJobs);
        writeHashMap(bw, partitionForAllJobs);
        writeHashMap(bw, costEffectiveness);
        writeHashMap(bw, logCostEffectiveness);
        writeHashMap(bw, directCostEffectiveness);
        writeHashMap(bw, logDirectCostEffectiveness);
        bw.close();
    }

    public static void writeSketchStatistics(List<JobStartEvent> jobs, String fileName, List<StageCompletedEvent> stages) throws Exception{
        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName)); // "ref_time_space_ce"
        //Map<Long, Integer> refForAllJobs = CacheSketcher.generateRefForJobs(jobs); // 可以不要
        Set<Long> actualStages = new HashSet<>();
        for(StageCompletedEvent sce : stages) {
            assert !actualStages.contains(sce.stage.stageId);
            actualStages.add(sce.stage.stageId);
        }
        Map<Long, Integer> directRefForAllJobs = CacheSketcher.generateRDDDirectRefForJobs(jobs, actualStages);
        //Map<Long, Integer> hopComputeTimeForAllJobs = CacheSketcher.generateHopComputeTimeForJobs(jobs); //可以是rdd id
//        Map<Long, Long> partitionForAllJobs = CacheSketcher.generatePartitionForJobs(jobs);
//        Map<Long, Float> costEffectiveness = new HashMap<>();
//        Map<Long, Float> logCostEffectiveness = new HashMap<>();
//        Map<Long, Float> directCostEffectiveness = new HashMap<>();
//        Map<Long, Float> logDirectCostEffectiveness = new HashMap<>();
//        for(long rddId : refForAllJobs.keySet()) {
//            costEffectiveness.put(rddId, (refForAllJobs.get(rddId) - 1) * hopComputeTimeForAllJobs.get(rddId)
//                    / (float) (partitionForAllJobs.get(rddId)));
//        }
//        for(long rddId : refForAllJobs.keySet()) {
////            float value = Math.max(0, (refForAllJobs.get(rddId) - 1) * (float) Math.log(hopComputeTimeForAllJobs.get(rddId)
////                    / (float) (partitionForAllJobs.get(rddId))));
//            float value = (refForAllJobs.get(rddId) - 1) * (float) Math.log(hopComputeTimeForAllJobs.get(rddId)
//                    / (float) (partitionForAllJobs.get(rddId)) + 1);
//            logCostEffectiveness.put(rddId, value);
//        }

//        for(long rddId : directRefForAllJobs.keySet()) {
//            directCostEffectiveness.put(rddId, (directRefForAllJobs.get(rddId) - 1) * hopComputeTimeForAllJobs.get(rddId)
//                    / (float) (partitionForAllJobs.get(rddId)));
//        }
//        for(long rddId : directRefForAllJobs.keySet()) {
////            float value = Math.max(0, (directRefForAllJobs.get(rddId) - 1) * (float) Math.log(hopComputeTimeForAllJobs.get(rddId)
////                    / (float) (partitionForAllJobs.get(rddId))));
//            float value = (directRefForAllJobs.get(rddId) - 1) * (float) Math.log(hopComputeTimeForAllJobs.get(rddId)
//                    / (float) (partitionForAllJobs.get(rddId)) + 1);
//            logDirectCostEffectiveness.put(rddId, value);
//        }
        //writeHashMap(bw, refForAllJobs);
        writeHashMap(bw, directRefForAllJobs);
        //writeHashMap(bw, hopComputeTimeForAllJobs);
//        writeHashMap(bw, partitionForAllJobs);
//        writeHashMap(bw, costEffectiveness);
//        writeHashMap(bw, logCostEffectiveness);
//        writeHashMap(bw, directCostEffectiveness);
//        writeHashMap(bw, logDirectCostEffectiveness);
        // choose RDD whose reference are greater than 2
        int totalSize = directRefForAllJobs.size();
        List<Long> biggerThanZero = new ArrayList<>();
        for(Map.Entry<Long, Integer> entry : directRefForAllJobs.entrySet()) {
            if(entry.getValue() >= 2) {
                biggerThanZero.add(entry.getKey());
            }
            if(entry.getValue() == 0) {
                totalSize--;
            }
        }
        // TODO: change into V0
        int num = CacheSketcher.generateStageHitNumWithStagesV0(stages, biggerThanZero);
        double ratio = CacheSketcher.generateStageHitRatioWithJobs(jobs, biggerThanZero);
        int totalStageNumOfJobs = SimpleUtil.stageNumOfJobs(jobs);
        // total_rdd_num, chose_rdd_num, total_stage_num, hit_stage_num, job_stage_hit_ratio
        System.out.println(totalSize + " " + biggerThanZero.size() + " " + stages.size() + " " + num + " " + ratio);
        bw.write(biggerThanZero.size() + "/" + totalSize + " " + biggerThanZero + "\n");
        bw.write("stage level: " + (num / (double) stages.size()) + ", " + num + "/" + stages.size() + ", job level: " + ratio +
                        ", " + String.format("%d", (int)(totalStageNumOfJobs * ratio)) + "/" +  totalStageNumOfJobs + "\n");
        bw.close();
        // write all to one log file
        BufferedWriter hitInfo = new BufferedWriter(new FileWriter("direct_ref_hit_info", true));
        hitInfo.write(totalSize + " " + biggerThanZero.size() + " " + stages.size() + " " + num + " " + ratio + "\n");
        hitInfo.close();
    }

    /**
     * write stage hit info
     * write rdd's use info in every action
     * write timeline of rdd's use info (job and stage)
     * @param jobs
     * @param stages
     * @param fileName
     * @throws IOException
     */
    public static void writeStageHitInfo(List<JobStartEvent> jobs, List<StageCompletedEvent> stages, String fileName) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
        int[][] simpleDAG = CacheSketcher.generateSimpleDAGByJobsAndStages(jobs, stages);
        List<Long> biggerThanZero = new ArrayList<>();
        int zeroRDDNum = 0;
        for(int i = 0; i < simpleDAG.length - jobs.size(); i++) {
            int sum = 0;
            for(int j = 0; j < simpleDAG[0].length; j++) {
                if(simpleDAG[i][j] > 0) {
                    sum += 1;
                }
            }
            if(sum > 1) {
                biggerThanZero.add((long) i);
            }
            if(sum == 0) {
                zeroRDDNum++;
            }
        }
        // TODO: write to file
        int num = CacheSketcher.generateStageHitNumWithStagesV1(stages, biggerThanZero);
        double ratio = CacheSketcher.generateStageHitRatioWithJobs(jobs, biggerThanZero);
        int totalStageNumOfJobs = SimpleUtil.stageNumOfJobs(jobs);
//        System.out.println((simpleDAG.length - jobs.size() - zeroRDDNum) + " " + biggerThanZero.size()
//                + " " + stages.size() + " " + num + " " + ratio);
        bw.write(biggerThanZero.size() + "/" + (simpleDAG.length - jobs.size() - zeroRDDNum)
                + " " + biggerThanZero + "\n");
        bw.write("stage level: " + (num / (double) stages.size()) + ", " + num + "/" + stages.size() + ", job level: " + ratio +
                ", " + String.format("%d", (int)(totalStageNumOfJobs * ratio)) + "/" +  totalStageNumOfJobs + "\n");
        bw.close();
        // write all to one log file
        BufferedWriter hitInfo = new BufferedWriter(new FileWriter("pure_ref_hit_info", true));
        hitInfo.write((simpleDAG.length - jobs.size() - zeroRDDNum) + " " + biggerThanZero.size()
                + " " + stages.size() + " " + num + " " + ratio + "\n");
        hitInfo.close();
        // write the chose RDD every job used
        // FIXME: write for the phenomenon `in same job/action`
        BufferedWriter rddToActions = new BufferedWriter(new FileWriter(fileName + "_rdd_to_actions", true));
        Set<Long> actualStages = new HashSet<>();
        for(StageCompletedEvent sce : stages) {
            actualStages.add(sce.stage.stageId);
        }
        Map<Long, String> rddToStagesInfo = new HashMap<>();
        Map<Long, List<Long>> actionToStages = new HashMap<>();// 记录action to stage(1对多)
        for(Long rddId : biggerThanZero) {
            StringBuilder sb = new StringBuilder();
            rddToActions.write("RDD_" + rddId + ": ");
            for(int i = 0; i < jobs.size(); i++) {
                JobStartEvent job = jobs.get(i);
                for(Stage stage : job.stages) {
                    // FIXME: 过滤stage
//                    System.out.println(i + ", " + stage.stageId);
                    if(!actualStages.contains(stage.stageId)) {
                        continue;
                    }
                    boolean hasMarked = false; // TODO: an bug
                    for(RDD rdd : stage.rdds) {
                        if(rdd.rddId.equals(rddId)) {
                            rddToActions.write("action_" + i + " ");
                            hasMarked = true;
                            break;
                        }
                    }
//                    if(hasMarked) {
//                        break;
//                    }
                    if(hasMarked) {
                        sb.append("action_").append(i).append("_stage_").append(stage.stageId).append(" ");
                        List<Long> curStages = actionToStages.getOrDefault((long) i, new ArrayList<>());
                        if(!curStages.contains(stage.stageId)) {
                            curStages.add(stage.stageId);
                        }
                        actionToStages.put((long) i, curStages);
                    }
                }
//                System.out.println(actionToStages.get((long) i));
            }
            rddToActions.write("\n");
            rddToStagesInfo.put(rddId, sb.toString());
        }
        for(int i = 0; i < jobs.size(); i++) {
            Collections.sort(actionToStages.get((long) i)); // 排个序
        }
        // 打印stage
        for(Long rddId : biggerThanZero) {
            rddToActions.write("RDD_" + rddId + ": " + rddToStagesInfo.get(rddId) + "\n");
        }
        // 打印timeline
        BufferedWriter csvWriter = new BufferedWriter(new FileWriter(fileName + ".csv"));
        csvWriter.write("job,stage");
        for(long rddId : biggerThanZero) {
            csvWriter.write(",RDD_" + rddId);
        }
        csvWriter.write("\n");
        for(long i = 0; i < jobs.size(); i++) {
            for(long j : actionToStages.get(i)) {
                // action i, stage j
                String key = "action_" + i + "_stage_" + j;
                StringBuilder toPrint = new StringBuilder(i + "," + j);
                for(long rddId : biggerThanZero) {
                    if(rddToStagesInfo.get(rddId).contains(key)) {
                        toPrint.append(",1");
                    }else{
                        toPrint.append(",0");
                    }
                }
                csvWriter.write(toPrint.toString() + "\n");
            }
        }
        csvWriter.write("\n\n");
        csvWriter.write("job,stage");
        for(long rddId : biggerThanZero) {
            csvWriter.write(",RDD_" + rddId);
        }
        csvWriter.write("\n");
        for(long i = 0; i < jobs.size(); i++) {
            // action i, stage j
            String key = "action_" + i;
            StringBuilder toPrint = new StringBuilder(i + ",");
            for(long rddId : biggerThanZero) {
                if(rddToStagesInfo.get(rddId).contains(key)) {
                    toPrint.append(",1");
                }else{
                    toPrint.append(",0");
                }
            }
            csvWriter.write(toPrint.toString() + "\n");
        }
        csvWriter.close();
        // end打印timeline csv
        rddToActions.close();
        // end FIXME: write for the phenomenon `in same job/action`
    }

    /**
     * 生成graph viz可读取的gv格式
     * @param jobs
     * @param stages
     * @param fileName
     * @throws IOException
     */
    public static void writeSimpleDAGUI(List<JobStartEvent> jobs, List<StageCompletedEvent> stages, String fileName) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
        int[][] simpleDAG = CacheSketcher.generateSimpleDAGByJobsAndStages(jobs, stages);
        // need: 1. all node num; 2. node name; 3.node dependency
        int zeroRDDNum = 0;
        Map<Integer, String> idToAction = new HashMap<>();
        int action = 0;
        for(int i = simpleDAG.length - jobs.size(); i < simpleDAG.length; i++) {
            idToAction.put(i, "action_" + action++);
        }
        List<String> nodeNames = new ArrayList<>();
        List<String> nodeDependency = new ArrayList<>();
        List<String> importantNode = new ArrayList<>();
        for(int i = 0; i < simpleDAG.length - jobs.size(); i++) {
            int sum = 0;
            for(int j = 0; j < simpleDAG[0].length; j++) {
                if(simpleDAG[i][j] > 0) {
                    sum += 1;
                    String tmp = i + " -> " +
                            (idToAction.containsKey(j) ? idToAction.get(j) : j);
                    nodeDependency.add(tmp);
                }
            }
            if(sum > 1) {
                importantNode.add(i + " [color = green, style = filled]");
            }
            if(sum == 0) {
                zeroRDDNum++;
            }else{
                nodeNames.add(String.valueOf(i));
            }
        }
        for(int i = simpleDAG.length - jobs.size(); i < simpleDAG.length; i++) {
            nodeNames.add(idToAction.get(i));
        }
//        bw.write((simpleDAG.length - zeroRDDNum) + "\n");
//        for(String s : nodeNames) {
//            bw.write(s + "\n");
//        }
        bw.write("digraph pic0{\n");
        bw.write("\trankdir=LR\n");
        for(String s : importantNode) {
            bw.write("\t" + s + "\n");
        }
        for(String s : nodeDependency) {
            bw.write("\t" + s + "\n");
        }
        bw.write("}");
        bw.close();
    }

    // TODO: write the split number of Stage

    // TODO: write cache ratio
    public static void writeCacheRatio() {

    }

    public static void writeHashMap(BufferedWriter bw, Map map) throws IOException {
        if(map.size() == 0) {
            return;
        }
        List<Long> keySet = new ArrayList<>(map.keySet());
        Collections.sort(keySet);
        for(int i = 0; i < keySet.size() - 1; i++) {
//            bw.write(map.get(keySet.get(i)).toString() + " ");
            bw.write("(" + keySet.get(i) + ", " + map.get(keySet.get(i)).toString() + ") ");
        }
//        bw.write(map.get(keySet.get(keySet.size() - 1)).toString() + "\n");
        bw.write("(" + keySet.get(keySet.size() - 1) + ", " + map.get(keySet.get(keySet.size() - 1)).toString() + ")\n");
    }

    /**
     * 根据选择缓存的RDD组成的集合的生成幂集，讨论每一种选择下，采用正常、longest path、 shortest path的总时间、缓存后时间和缓存节省时间
     * @param choseRDD
     * @param job
     * @param fileName
     * @throws IOException
     */
    // KEYPOINT: bw need to close out of this function
    public static void writeCacheToTime(List<Long> choseRDD, JobStartEvent job, String applicationName, String fileName) throws IOException {
        // write: choseRDD, initial time(1), choose longest path's initial time(2), choose shortest path time's initial time(3)
        // after cache time(4), choose longest path's after cache time(5), choose shortest path's after cache time(6)
        // after cache reduction time(1-4), choose longest path's after cache reduction time(2-5), xxx shortest xxx (3-6)
        // KEYPOINT: 要做就做一个通用的出来
        Stage lastStage = SimpleUtil.lastStageOfJob(job);
        Map<Long, Stage> stageMap = SimpleUtil.stageMapOfJob(job);
        BufferedWriter bw = new BufferedWriter(new FileWriter(applicationName + fileName, true));
        Arrays.fill(sum, 0);
        bw.write(applicationName + ": job_" + job.jobId + "\n");
//        bw.write("toCache, A_all, L_all, S_all, +_all, " +
//                "A_cache, L_cache, S_cache, +_cache, " +
//                "A_reduce, L_reduce, S_reduce, +_reduce\n");TODO: recover writing
        backtrack(choseRDD, 0, lastStage, stageMap, bw, new ArrayList<>());
        for (long l : sum) {
            bw.write("," + l); // for csv
//            System.out.print(sum[i] + ",");
        }
        bw.write("," + "record sum");
//        System.out.println("sum...");
        bw.write("\n\n\n");
        bw.close();
        // for CDF: key_path_cdf_sum.csv
        // sum: |[4]-[5]|/[4], |[4]-[7]|/[4], |[8]-[9]|/[8], |[8]-[11]|/[8]
        BufferedWriter sumCDF = new BufferedWriter(new FileWriter("key_path_cdf_sum.csv", true) );
        sumCDF.write(applicationName + ": job_" + job.jobId + "\n");
//        System.out.println(SimpleUtil.generateDifferenceRatio(sum[4], sum[5]));
        sumCDF.write(String.format(doublePrintFormat, SimpleUtil.generateDifferenceRatio(sum[4], sum[5])));
        sumCDF.write("," + String.format(doublePrintFormat, SimpleUtil.generateDifferenceRatio(sum[4], sum[7])));
        sumCDF.write("," + String.format(doublePrintFormat, SimpleUtil.generateDifferenceRatio(sum[8], sum[9])));
        sumCDF.write("," + String.format(doublePrintFormat, SimpleUtil.generateDifferenceRatio(sum[8], sum[11])) + "\n");
        sumCDF.close();
        // end CDF: key_path_cdf_sum.csv
    }

    /**
     *根据jobs和stages得到represent time，然后根据rddToCache的每种情况计算all time，write job-level的all/represent time+几个job的all/represent time
     * @param jobs
     * @param stages
     * @param rddToCache
     * @param applicationName
     * @throws IOException
     */
    public static void writeTotalAndRepresentTime(List<JobStartEvent> jobs, List<Stage> stages, List<Long> rddToCache, String applicationName) throws IOException {
        // step 1. get every data's representative time
        // step 2. get the process of total time (use key path) of different cached data// KEYPOINT maybe every job
        Arrays.fill(ALL_REPRESENT, 0);
        Map<Long, List<Double>> representTime = SimpleUtil.representTimeOfCachedRDDAmongJobsAndStages(stages, rddToCache);
        for(JobStartEvent job : jobs) {
            // 每个job
            Stage lastStage = SimpleUtil.lastStageOfJob(job);
            Map<Long, Stage> stageMap = SimpleUtil.stageMapOfJob(job);
            // step 3. write the result of every job or all job separately
            backtrackV2(applicationName, rddToCache, 0, lastStage, stageMap, new ArrayList<>(), representTime); // TODO: add back
        }
        // step 4. plus the time of different cached data from representative data
        BufferedWriter allJobCompare = new BufferedWriter(new FileWriter("all_vs_represent_all_job.csv", true) );
        allJobCompare.write(applicationName + ": start from job_" + jobs.get(0).jobId + "\n");
        allJobCompare.write("," + ALL_REPRESENT[0]);
        allJobCompare.write("," + ALL_REPRESENT[1]);
        allJobCompare.write("," + String.format(doublePrintFormat, SimpleUtil.generateDifferenceRatio(ALL_REPRESENT[0], ALL_REPRESENT[1])) + "\n");
        allJobCompare.close();
    }

    /**
     * 对于选中的缓存集合，得到一个longest path的run time，同时记录根据map得到的run time，两者做一个差异
     * @param choseRDD
     * @param end
     * @param stage
     * @param stageMap
     * @param tmp
     * @throws IOException
     */
    private static void backtrackV2(String applicationName, List<Long> choseRDD, int end, Stage stage, Map<Long, Stage> stageMap,
                                    List<Long> tmp, Map<Long, List<Double>> representTime) throws IOException {
        // end为开区间
        if(end > choseRDD.size()) {
            return;
        }
        Set<Long> toPerform = new HashSet<>(tmp);
        int one = SimpleUtil.computeTimeOfStage(stage, stageMap) -
                SimpleUtil.computeTimeOfStageWithCacheByLSPath(toPerform, stage, stageMap, true);
        double two = SimpleUtil.computeTimeOfStage(stage, stageMap) -
                SimpleUtil.computeTimeOfStageWithCacheByRepresentTime(toPerform, stage, stageMap, true, representTime);
//        for(long rddId : toPerform) {
//            if(representTime.containsKey(rddId)) {
//                two += NumberUtil.mean(representTime.get(rddId));
//            }
//        }
//        for(Stage stg : stageMap.values()) {
//            stg.rdds.sort((o1, o2) -> (int) (o1.rddId - o2.rddId));
//            for(int j = stg.rdds.size() - 1; j >= 0; j--) {
//                long rddId = stg.rdds.get(j).rddId;
//                if(toPerform.contains(rddId)) {
////                    if(representTime.containsKey(rddId)) {
////                        two += NumberUtil.mean(representTime.get(rddId)); // TODO: 平均值还可以再优化
////                    }
////                    two += (j + 1); TODO: 要找关键路径
//                    break;
//                }
//            }
//        }
        double[] tmpArr = {one, two};
        for(int i = 0; i < ALL_REPRESENT.length; i++) {
            ALL_REPRESENT[i] += tmpArr[i];
        }
        StringBuilder sb = new StringBuilder("[");
        for(long l : tmp) {
            sb.append(l).append(" ");
        }
        if(sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("]");
        BufferedWriter everyJobCompare = new BufferedWriter(new FileWriter("all_vs_represent_every_job.csv", true) );
        everyJobCompare.write(applicationName + "_stage_" + stage.stageId + "_" + sb.toString());
        everyJobCompare.write("," + one);
        everyJobCompare.write("," + two);
        everyJobCompare.write("," + String.format(doublePrintFormat, SimpleUtil.generateDifferenceRatio(tmpArr[0], tmpArr[1])) + "\n");
        everyJobCompare.close();
        for(int i = end; i < choseRDD.size(); i++) {
            // dfs
            tmp.add(choseRDD.get(i));
            backtrackV2(applicationName, choseRDD, i + 1, stage, stageMap, tmp, representTime);
            tmp.remove(tmp.size() - 1);
        }
    }

    private static void backtrack(List<Long> choseRDD, int end, Stage stage, Map<Long, Stage> stageMap, BufferedWriter bw, List<Long> tmp) throws IOException {
        // end为开区间
        if(end > choseRDD.size()) {
            return;
        }
        Set<Long> toPerform = new HashSet<>(tmp);
        int one = SimpleUtil.computeTimeOfStage(stage, stageMap), two = one;
        int three = SimpleUtil.computeTimeOfStageWithShortestPath(stage, stageMap);
        int threeAfter = SimpleUtil.computeTimeOfStageWithAccumulation(stage, stageMap);
        int four = SimpleUtil.computeTimeOfStageWithCache(toPerform, stage, stageMap);
        int five = SimpleUtil.computeTimeOfStageWithCacheByLSPath(toPerform, stage, stageMap, true);
        int six = SimpleUtil.computeTimeOfStageWithCacheByLSPath(toPerform, stage, stageMap, false);
        int sixAfter = SimpleUtil.computeTimeOfStageWithCacheAndAccumulation(toPerform, stage, stageMap);
        int seven = one - four, eight = two - five, nine = three - six, nineAfter = threeAfter - sixAfter;
        int[] tmpArr = {one, two, three, threeAfter, four, five, six, sixAfter, seven, eight, nine, nineAfter};
        for(int i = 0; i < sum.length; i++) {
            sum[i] += tmpArr[i];
        }
        StringBuilder sb = new StringBuilder("[");
        for(long l : tmp) {
            sb.append(l).append(" ");
        }
        if(sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("]");
//        String toWrite = sb.toString() + ", " + one + ", " + two + ", " + three +
//                ", " + threeAfter + ", " + four + ", " + five + ", " + six + ", " +
//                sixAfter + ", " + seven + ", " + eight + ", " + nine + "," + nineAfter + "\n";
////        System.out.print(toWrite);
//        bw.write(toWrite); TODO: recover writing
        // for CDF: key_path_cdf_detail.csv
        // |four-five|/four, |four-sixAfter|/four, |eight-seven|/seven, |nineAfter-seven|/seven TODO: divided by zero
        // tmpArr: |[4]-[5]|/[4], |[4]-[7]|/[4], |[8]-[9]|/[8], |[8]-[11]|/[8]
        BufferedWriter detailCDF = new BufferedWriter(new FileWriter("key_path_cdf_detail.csv", true) );
        detailCDF.write(sb.toString());
        detailCDF.write("," + String.format(doublePrintFormat, SimpleUtil.generateDifferenceRatio(tmpArr[4], tmpArr[5])));
        detailCDF.write("," + String.format(doublePrintFormat, SimpleUtil.generateDifferenceRatio(tmpArr[4], tmpArr[7])));
        detailCDF.write("," + String.format(doublePrintFormat, SimpleUtil.generateDifferenceRatio(tmpArr[8], tmpArr[9])));
        detailCDF.write("," + String.format(doublePrintFormat, SimpleUtil.generateDifferenceRatio(tmpArr[8], tmpArr[11])) + "\n");
        detailCDF.close();
        // end CDF: key_path_cdf_detail.csv
        for(int i = end; i < choseRDD.size(); i++) {
            // dfs
            tmp.add(choseRDD.get(i));
            backtrack(choseRDD, i + 1, stage, stageMap, bw, tmp);
            tmp.remove(tmp.size() - 1);
        }
    }

    /**
     * 写入每个stage中，rdd的partition信息到bw
     * @param applicationName
     * @param job
     * @param stage
     * @param bw
     * @throws IOException
     */
    public static void stageAllRDDPartition(String applicationName, JobStartEvent job, Stage stage, BufferedWriter bw) throws IOException {
        StringBuilder sb = new StringBuilder(applicationName + "/" + job.jobId + "/" + stage.stageId);
        List<Double> data = new ArrayList<>();
        for(RDD rdd : stage.rdds) {
            sb.append(", ").append(rdd.partitionNum);
            data.add((double) rdd.partitionNum);
        }
        bw.write(sb.toString() + ", " + NumberUtil.mean(data) + ", "
                + NumberUtil.stdDev(data) + "\n");
    }

    /**
     * 写入每个stage中缓存的rdd的partition信息到bw
     * simpleDAG和jobSize用于计算每个stage中的cached rdd
     * @param applicationName
     * @param job
     * @param stage
     * @param simpleDAG
     * @param jobSize
     * @param bw
     */
    public static void stageCachedRDDPartition(String applicationName, JobStartEvent job, Stage stage,
                                               int[][] simpleDAG, int jobSize, BufferedWriter bw) throws IOException {
        // step 1. get cached rdd
        Map<Long, RDD> rddInStageMap = new HashMap<>();
        List<Long> toBeCache = new ArrayList<>();
        for(RDD rdd : stage.rdds) {
            assert !rddInStageMap.containsKey(rdd.rddId);
            rddInStageMap.put(rdd.rddId, rdd);
        }
        for(int i = 0; i < simpleDAG.length - jobSize; i++) {
            if(!rddInStageMap.containsKey((long) i)) {
                continue;
            }
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
        // step 2. record partition info
        StringBuilder sb = new StringBuilder(applicationName + "/" + job.jobId + "/" + stage.stageId);
        List<Double> data = new ArrayList<>();
        for(long rddId : toBeCache) {
            sb.append(", ").append(rddInStageMap.get(rddId).partitionNum);
            data.add((double) rddInStageMap.get(rddId).partitionNum);
        }
        bw.write(sb.toString() + ", " + NumberUtil.mean(data) + ", "
                + NumberUtil.stdDev(data) + "\n");
    }

    private static void swap(int i, int j, List<Long> choseRDD) {
        if(i == j) {
            return;
        }
        long tmp = choseRDD.get(i);
        choseRDD.set(i, choseRDD.get(j));
        choseRDD.set(j, tmp);
    }

    public static void main(String[] args) {

    }
}
