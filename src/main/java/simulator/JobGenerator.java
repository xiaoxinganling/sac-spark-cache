package simulator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import entity.Job;
import entity.RDD;
import entity.Stage;
import entity.event.StageCompletedEvent;
import sketch.StaticSketch;

import java.io.*;
import java.util.*;

public class JobGenerator {

    public final static String STAGE_CPU = "a_a_stage_cpu_info";

    public final static int CPU_LOW_BOUND = 2;

    public final static int CPU_UP_BOUND = 7;

    /**
     * 根据application文件名读取包含所有stage的job list(stage list无序)
     * @param fileName
     * @return
     * @throws IOException
     */
    @Deprecated
    public static List<Job> generateJobsWithAllStagesOfApplication(String fileName) throws IOException {
        List<Job> jobList = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String line;
        while((line = br.readLine()) != null) {
            JSONObject jsonObject = JSONObject.parseObject(line);
            if(jsonObject.get("Event").equals(StaticSketch.jobStartEventFlag)) {
                jobList.add(JSON.toJavaObject(jsonObject, Job.class));
            }
        }
        br.close();
        return jobList;
    }

    /**
     * 根据application文件名读取包含过滤后stage的job list（stage list 无序）
     * @param fileName
     * @return
     * @throws IOException
     */
    public static List<Job> generateJobsWithFilteredStagesOfApplication(String fileName) throws IOException {
        String newFileName = fileName + "_new";
        if(new File(newFileName).exists()) {
            List<Job> resList = new ArrayList<>();
            BufferedReader br = new BufferedReader(new FileReader(newFileName));
            String line;
            while((line = br.readLine()) != null) {
                Job curJob = JSON.toJavaObject(JSONObject.parseObject(line), Job.class);
                resList.add(curJob);
            }
            br.close();
            return resList;
        }
        List<Job> jobList = new ArrayList<>();
        Map<Long, Stage> actualStages = new HashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String line;
        while((line = br.readLine()) != null) {
            JSONObject jsonObject = JSONObject.parseObject(line);
            if(jsonObject.get("Event").equals(StaticSketch.stageCompletedEventFlag)) {
                StageCompletedEvent sce = JSON.toJavaObject(jsonObject, StageCompletedEvent.class);
                actualStages.put(sce.stage.stageId, sce.stage);
            }else if(jsonObject.get("Event").equals(StaticSketch.jobStartEventFlag)) {
                jobList.add(JSON.toJavaObject(jsonObject, Job.class));
            }
        }
        br.close();
        for (Job job : jobList) {
             List<Stage> filteredStageList = new ArrayList<>();
             List<Long> stageIds = new ArrayList<>();
             for (Stage stage : job.stages) {
                 if (actualStages.containsKey(stage.stageId)) {
                     // KEYPOINT: Stage 无重复
                     // init compute time for RDD
                     Stage fullStage = actualStages.get(stage.stageId);
                     long stageComputeTime = fullStage.completeTime - fullStage.submitTime;
                     double avgTime = stageComputeTime / (double) fullStage.rdds.size();
                     for (RDD rdd : fullStage.rdds) {
                         rdd.computeTime = avgTime;
                     }
                     // end init
                     filteredStageList.add(fullStage);
                     stageIds.add(stage.stageId);
                 }
             }
             job.stages = filteredStageList;
             job.stageIds = stageIds;
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter(newFileName));
        for (Job job : jobList) {
            bw.write(JSON.toJSON(job).toString() + "\n");
        }
        bw.close();
        return jobList;
    }


    public static void updateStageCPUResourceOfJobList(List<Job> jobList, String application) throws IOException {
        boolean existCPUInfo = new File(String.format("%s_%s", STAGE_CPU, application)).exists();
        if (existCPUInfo) {
            BufferedReader br = new BufferedReader(new FileReader(String.format("%s_%s", STAGE_CPU, application)));
            Map<Long, Integer> CPUMap = new HashMap<>();
            String line;
            while ((line = br.readLine()) != null) {
                long key = Long.parseLong(line.split("_")[0]);
                int value = Integer.parseInt(line.split("_")[1]);
                assert !CPUMap.containsKey(key);
                CPUMap.put(key, value);
            }
            br.close();
            for (Job job : jobList) {
                for (Stage stage : job.stages) {
                    stage.needCPU = CPUMap.get(stage.stageId);
                }
            }
            return;
        }
        BufferedWriter bw = new BufferedWriter(new FileWriter(String.format("%s_%s", STAGE_CPU, application)));
        for (Job job : jobList) {
            for (Stage stage : job.stages) {
                int cpu = new Random().nextInt(CPU_UP_BOUND - CPU_LOW_BOUND) + CPU_LOW_BOUND;
                stage.needCPU = cpu;
                bw.write(String.format("%d_%d\n", stage.stageId, cpu));
            }
        }
        bw.close();
    }

}