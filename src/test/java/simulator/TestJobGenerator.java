package simulator;

import entity.Job;
import entity.RDD;
import entity.Stage;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import java.io.IOException;
import java.util.*;

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
                    long completeTime = stage.completeTime - stage.submitTime;
                    double avgTime = completeTime / (double) stage.rdds.size();
                    System.out.println(String.format("stage_%d, computeTime: %d, rdd_size %d, avg time: %f",
                            stage.stageId, completeTime, stage.rdds.size(), avgTime));
                    for (RDD rdd : stage.rdds) {
                        assertEquals(avgTime, rdd.computeTime);
                    }
                    stageIdSet.add(stage.stageId);
                }
                //System.out.println("job_" + job.jobId + "_stage_num: " + job.stages.size() + " " + stageIdSet);
            }
        }
    }

    @Test
    void testTaskBlockRelation() throws IOException {

        for(int i = 0; i < applicationName.length; i++) {
//            if(!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
            int totalSerialAndParallel = 0;
            int totalSerial = 0;
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            System.out.println(applicationName[i] + " " + jobList.size());
            for(Job job : jobList) {
                for(Stage stage : job.stages) {
                    Map<Long, RDD> rddInOneStage = new HashMap<>();
                    for (RDD rdd : stage.rdds) {
                        rddInOneStage.put(rdd.rddId, rdd);
                    }
                    for (RDD rdd : stage.rdds) {
                        int validParentSize = 0;
                        for (long parentId : rdd.rddParentIDs) {
                            if (rddInOneStage.containsKey(parentId)) {
                                validParentSize++;
                            }
                        }
                        if (validParentSize > 1) {
                            totalSerialAndParallel++;
                            long totalPartitionNum = 0;
                            for (long parentId : rdd.rddParentIDs) {
                                if (rddInOneStage.containsKey(parentId)) {
                                    totalPartitionNum += rddInOneStage.get(parentId).partitionNum;
                                }
                            }
//                            System.out.println(String.format("job_%d_stage_%d: %d <- %s", job.jobId,
//                                    stage.stageId, rdd.rddId, rdd.rddParentIDs));
                            if (totalPartitionNum == rdd.partitionNum) {
                                totalSerial++;
                            } else {
//                                System.out.println(String.format("Unresolved: job_%d_stage_%d: %d <- %s", job.jobId,
//                                        stage.stageId, rdd.rddId, rdd.rddParentIDs));
                            }
//                            assertEquals(totalPartitionNum, rdd.partitionNum);
                        }
                    }
                }
                //System.out.println("job_" + job.jobId + "_stage_num: " + job.stages.size() + " " + stageIdSet);
            }
            System.out.println(String.format("Ratio of Serial: %d / %d = %.2f", totalSerial, totalSerialAndParallel,
                    (totalSerial / (double) totalSerialAndParallel)));
        }
    }
}