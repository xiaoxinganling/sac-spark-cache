package simulator;

import entity.Job;
import entity.RDD;
import entity.Stage;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import utils.SimpleUtil;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static simulator.RDDStageInfoManager.MAX_DISTANCE;

class TestRDDStageInfoManager {

    // KEYPOINT: 这也是个很好的辅助功能： rddIdToStageIds, stageIdToRDDIds
    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    String fileName = "E:\\Google Chrome Download\\";
//    String[] fileNames = {StaticSketch.applicationPath[5]};
//    String[] applicationNames = {applicationName[5]}; // svm 5, svm, 永远滴工具人！
    String[] fileNames = applicationPath;
    String[] applicationNames = applicationName;

    @Test
    void testGenerateDistanceForHotData() throws IOException {
        for (int i = 0; i < fileNames.length; i++) {
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
            List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList);
            Map<Long, PriorityQueue<Long>> rddToStageIds = RDDStageInfoManager.generateDistanceForHotData(jobList, hotData);
            System.out.println(rddToStageIds);
            System.out.println(RDDStageInfoManager.generateStageToRDDIds(rddToStageIds));
        }
    }

    @Test
    void testUpdateDistance() throws IOException {
        for (int i = 0; i < fileNames.length; i++) {
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
            List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList);
            Map<Long, PriorityQueue<Long>> rddToStageIds = RDDStageInfoManager.generateDistanceForHotData(jobList, hotData);
            Set<Long> hotDataIds = new HashSet<>();
            for (RDD rdd : hotData) {
                hotDataIds.add(rdd.rddId);
            }
            System.out.println(rddToStageIds);
            for (Job job : jobList) {
                // 肯定是执行顺序的一种
                job.stages.sort((o1, o2) -> (int) (o1.stageId - o2.stageId)); // FIXME: 目前还是按照从小到大的顺序来
                for (Stage stage : job.stages) {
                    // 肯定会报错 => ? 竟然没报错
                    System.out.println("asserting stage " + stage.stageId + " -> " + SimpleUtil.cachedRDDIdSetInStage(stage, hotDataIds));
                    RDDStageInfoManager.updateDistance(rddToStageIds, stage);
                    System.out.println(rddToStageIds);
                }
            }
            for (PriorityQueue<Long> value : rddToStageIds.values()) {
                assertEquals(1, value.size());
                assertEquals(MAX_DISTANCE, value.peek());
            }
        }
    }



}