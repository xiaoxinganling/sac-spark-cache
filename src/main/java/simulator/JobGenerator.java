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
}
