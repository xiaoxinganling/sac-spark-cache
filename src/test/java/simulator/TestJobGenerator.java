package simulator;

import entity.Job;
import entity.Stage;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TestJobGenerator {

    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    String fileName = "E:\\Google Chrome Download\\";

    @Test
    void generateJobsWithAllStagesOfApplication() throws IOException {
        for(int i = 0; i < applicationName.length; i++) {
            if(!applicationName[i].contains("spark_svm")) {
                continue;
            }
            List<Job> jobList = JobGenerator.generateJobsWithAllStagesOfApplication(fileName + applicationPath[i]);
            System.out.println(applicationName[i] + " " + jobList.size());
            for(Job job : jobList) {
                Set<Long> stageIdSet = new HashSet<>();
                for(Stage stage : job.stages) {
                    stageIdSet.add(stage.stageId);
                }
                System.out.println("job_" + job.jobId + "_stage_num: " + job.stages.size() + " " + stageIdSet);
            }
        }
    }

    @Test
    void generateJobsWithFilteredStagesOfApplication() throws IOException {
        for(int i = 0; i < applicationName.length; i++) {
            if(!applicationName[i].contains("spark_svm")) {
                continue;
            }
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            System.out.println(applicationName[i] + " " + jobList.size());
            for(Job job : jobList) {
                Set<Long> stageIdSet = new HashSet<>();
                for(Stage stage : job.stages) {
                    stageIdSet.add(stage.stageId);
                }
                System.out.println("job_" + job.jobId + "_stage_num: " + job.stages.size() + " " + stageIdSet);
            }
        }
    }
}