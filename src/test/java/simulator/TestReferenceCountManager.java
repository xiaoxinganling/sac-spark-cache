package simulator;

import entity.Job;
import entity.RDD;
import entity.Stage;
import entity.event.JobStartEvent;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import utils.CacheSketcher;
import utils.SimpleUtil;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TestReferenceCountManager {


    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    String fileName = "E:\\Google Chrome Download\\";
//    String[] fileNames = {StaticSketch.applicationPath[5]};
//    String[] applicationNames = {applicationName[5]}; // svm 5
    String[] fileNames = applicationPath;
    String[] applicationNames = applicationName;

    @Test
    void generateRefCountForHotData() throws IOException {
        {
            for (int i = 0; i < fileNames.length; i++) {
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
                List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList);
                Set<Long> hotDataIds = new HashSet<>();
                List<JobStartEvent> jseList = new ArrayList<>(jobList);
                Map<Long, Integer> hotDataRC = ReferenceCountManager.generateRefCountForHotData(jobList, hotData);
                Map<Long, Integer> totalRC = CacheSketcher.generateRDDDirectRefForJobs(jseList, null);
                for (RDD rdd : hotData) {
                    hotDataIds.add(rdd.rddId);
                }
                System.out.println(hotDataIds + " -> " + hotDataRC);
                System.out.println(totalRC);//其实可打印可不打印
                assertEquals(hotDataIds.size(), hotDataRC.size());
                for(Map.Entry<Long, Integer> entry : hotDataRC.entrySet()) {
                    assertEquals(totalRC.get(entry.getKey()), entry.getValue());
                }
            }
        }
    }

    @Test
    void updateHotDataRefCount() throws IOException { // 吐槽一下，我这把别人的工作也实现得也太好了= =
        {
            // 测试最终hotDataRC所有value为0
            for (int i = 0; i < fileNames.length; i++) {
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
                List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList);
                Map<Long, Integer> rddIdToActionNum = ReferenceCountManager.generateRDDIdToActionNum(jobList);
                Map<Long, Integer> hotDataRC = ReferenceCountManager.generateRefCountForHotData(jobList, hotData);
                System.out.println(hotDataRC);
                for (Job job : jobList) {
                    System.out.println("job " + job.jobId +  " before: " + hotDataRC);
                    for (Stage stage: job.stages) {
                        for (RDD rdd : stage.rdds) {
                            ReferenceCountManager.updateHotDataRefCountByRDD(hotDataRC, rdd, rddIdToActionNum);
                        }
                    }
                    System.out.println("job " + job.jobId +  " after:  " + hotDataRC);
                }
                for (int value : hotDataRC.values()) {
                    assertEquals(0, value);
                }
            }
        }
    }

    @Test
    void updateHotDataRefCountByStage() throws IOException { // 吐槽一下，我这把别人的工作也实现得也太好了= =
        {
            // 测试最终hotDataRC所有value为0
            for (int i = 0; i < fileNames.length; i++) {
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
                List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList);
                Map<Long, Integer> rddIdToActionNum = ReferenceCountManager.generateRDDIdToActionNum(jobList);
                Map<Long, Integer> hotDataRC = ReferenceCountManager.generateRefCountForHotData(jobList, hotData);
                System.out.println(hotDataRC);
                for (Job job : jobList) {
                    System.out.println("job " + job.jobId +  " before: " + hotDataRC);
                    for (Stage stage : job.stages) {
                        ReferenceCountManager.updateHotDataRefCountByStage(hotDataRC, stage, rddIdToActionNum);
                    }
                    System.out.println("job " + job.jobId +  " after:  " + hotDataRC);
                }
                for (int value : hotDataRC.values()) {
                    assertEquals(0, value);
                }
            }
        }
    }

}