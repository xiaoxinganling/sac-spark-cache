package simulator.dp;

import entity.Job;
import entity.RDD;
import entity.Stage;
import org.junit.jupiter.api.Test;
import simulator.HotDataGenerator;
import simulator.JobGenerator;
import sketch.StaticSketch;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TestRDDTimeManager {

    String fileName = "E:\\Google Chrome Download\\";
    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;

    @Test
    void testCachedRDDTimeWithKeyStages() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
//            if (!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            List<RDD> hotRDD = HotDataGenerator.hotRDD(applicationName[i], jobList);
            Set<Long> hotRDDIds = new HashSet<>();
            for (RDD rdd : hotRDD) {
                hotRDDIds.add(rdd.rddId);
            }
            Map<Long, Stage> keyStages = KeyPathManager.generateKeyStages(jobList);
//            for (long key : keyStages.keySet()) {
//                Stage stage = keyStages.get(key);
//                stage.rdds.sort((o1, o2) -> (int) (o2.rddId - o1.rddId));
//            }
//            for (Stage stage : keyStages.values()) {
//                stage.rdds.sort((o1, o2) -> (int) (o2.rddId - o1.rddId));
//            }
            for (Job job : jobList) {
                for (Stage stage : job.stages) {
//                    System.out.println("Before running stage: " + stage.stageId);
//                    System.out.println(RDDTimeManager.cachedRDDTimeWithKeyStages(keyStages, hotRDDIds));
//                    System.out.println("After running stage: " + stage.stageId);
                    KeyPathManager.updateKeyStages(keyStages, stage);
//                    System.out.println(RDDTimeManager.cachedRDDTimeWithKeyStages(keyStages, hotRDDIds));
                }
            }
        }
    }
}