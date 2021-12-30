package simulator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import entity.Job;
import entity.Stage;
import entity.event.StageCompletedEvent;
import sketch.StaticSketch;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
        List<Job> jobList = new ArrayList<>();
        Set<Long> actualStageIds = new HashSet<>();
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String line;
        while((line = br.readLine()) != null) {
            JSONObject jsonObject = JSONObject.parseObject(line);
            if(jsonObject.get("Event").equals(StaticSketch.stageCompletedEventFlag)) {
                StageCompletedEvent sce = JSON.toJavaObject(jsonObject, StageCompletedEvent.class);
                actualStageIds.add(sce.stage.stageId);
            }else if(jsonObject.get("Event").equals(StaticSketch.jobStartEventFlag)) {
                jobList.add(JSON.toJavaObject(jsonObject, Job.class));
            }
        }
        br.close();
        for(Job job : jobList) {
             List<Stage> filteredStageList = new ArrayList<>();
             for(Stage stage : job.stages) {
                 if(actualStageIds.contains(stage.stageId)) {
                     // KEYPOINT: Stage 无重复
                     filteredStageList.add(stage);
                 }
             }
             job.stages = filteredStageList;
        }
        return jobList;
    }
}