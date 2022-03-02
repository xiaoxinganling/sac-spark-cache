package simulator;

import entity.Job;
import entity.RDD;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;

import java.io.IOException;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class TestHotDataGenerator {

    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    String fileName = "E:\\Google Chrome Download\\";
    int importantApp = 5;

    @Test
    void testHotRDD() throws IOException {
        {
//            String[] fileNames = {StaticSketch.applicationPath[importantApp]};
//            String[] applicationNames = {applicationName[importantApp]}; // svm 5
            String[] fileNames = applicationPath;
            String[] applicationNames = applicationName;
            for(int i = 0; i < fileNames.length; i++) {
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
                List<RDD> hotRDD = HotDataGenerator.hotRDD(applicationNames[i], jobList, null);
                System.out.println(applicationNames[i] + " -> " + hotRDD.size());
            }
        }
    }

    @Test
    void testTwiceRDD() throws IOException {
        {
            String[] fileNames = applicationPath;
            String[] applicationNames = applicationName;
            for(int i = 0; i < fileNames.length; i++) {
                List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + fileNames[i]);
                List<RDD> hotRDD = HotDataGenerator.twiceRDD(applicationNames[i], jobList);
                System.out.println(applicationNames[i] + " -> " + hotRDD.size());
            }
        }
    }

    @Test
    void testProposeCacheSpaceSize() throws IOException {
        String[] fileNames = {fileName + StaticSketch.applicationPath[5]};
        String[] applicationNames = {applicationName[5]}; // svm 5
        for (int i = 0; i < fileNames.length; i++) {
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileNames[i]);
            List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList, null);
            long proposedSize = HotDataGenerator.proposeCacheSpaceSize(applicationNames[i], hotData);
            long sum = 0;
            for (RDD rdd : hotData) {
                System.out.println(rdd.partitionNum);
                sum += rdd.partitionNum;
            }
            assertEquals(sum, proposedSize);
        }
    }

    @Test
    void testNewHotData() throws IOException {
        String[] fileNames = {fileName + StaticSketch.applicationPath[5]};
        String[] applicationNames = {applicationName[5]}; // svm 5
        for (int i = 0; i < fileNames.length; i++) {
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileNames[i]);
            ReplacePolicy[] replacePolicies = {ReplacePolicy.LRU, ReplacePolicy.LFU, ReplacePolicy.LRC, ReplacePolicy.MRD, ReplacePolicy.DP};
            for (ReplacePolicy rp : replacePolicies) {
                List<RDD> hotData = HotDataGenerator.hotRDD(applicationNames[i], jobList, rp);
                System.out.println(rp + " " + hotData.size());
            }
        }
    }

}