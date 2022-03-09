package simulator;

import entity.Job;
import entity.Partition;
import entity.RDD;
import entity.Task;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import task.TaskGenerator;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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


    @Test
    void testProposeTaskCacheSpace() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
            Map<Long, List<Task>> stageIdToTasks = TaskGenerator.generateTaskOfApplicationV2(
                    fileName + StaticSketch.applicationPath[i], null);
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + StaticSketch.applicationPath[i]);
            List<RDD> hotData = HotDataGenerator.hotRDD(fileName + StaticSketch.applicationPath[i], jobList, null);
            Set<Long> hotRDDIds = new HashSet<>();
            Set<String> hasAdded = new HashSet<>();
            for (RDD rdd : hotData) {
                hotRDDIds.add(rdd.rddId);
            }
            long proposedSize = HotDataGenerator.proposeTaskCacheSpace(applicationName[i], hotData, stageIdToTasks);
            long sum = 0;
            for (List<Task> tasks : stageIdToTasks.values()) {
                for (Task task : tasks) {
                    for (Partition p : task.getPartitions()) {
                        if (!hotRDDIds.contains(p.belongRDD.rddId) || hasAdded.contains(p.getPartitionId())) {
                            continue;
                        }
                        sum += p.getMemorySize();
                        hasAdded.add(p.getPartitionId());
                    }
                }
            }
            assertEquals(sum, proposedSize);
        }
    }

}