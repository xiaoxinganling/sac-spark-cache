package utils;

import entity.RDD;
import entity.Stage;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class ResultOutputer {

    public static void writeFullSketchStatistics(List<JobStartEvent> jobs, String fileName, List<StageCompletedEvent> stages) throws Exception{
        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName)); // "ref_time_space_ce"
        Map<Long, Integer> refForAllJobs = CacheSketcher.generateRefForJobs(jobs); // 可以不要
        Map<Long, Integer> directRefForAllJobs = CacheSketcher.generateRDDDirectRefForJobs(jobs);
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
        Map<Long, Integer> directRefForAllJobs = CacheSketcher.generateRDDDirectRefForJobs(jobs);
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
        int num = CacheSketcher.generateStageHitNumWithStages(stages, biggerThanZero);
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
        int num = CacheSketcher.generateStageHitNumWithStages(stages, biggerThanZero);
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

    public static void  writeHashMap(BufferedWriter bw, Map map) throws IOException {
        if(map.size() == 0) {
            return;
        }
        List<Long> keySet = new ArrayList<>(map.keySet());
        Collections.sort(keySet);
        for(int i = 0; i < keySet.size() - 1; i++) {
            bw.write(map.get(keySet.get(i)).toString() + " ");
        }
        bw.write(map.get(keySet.get(keySet.size() - 1)).toString() + "\n");
    }

    public static void main(String[] args) {

    }
}
