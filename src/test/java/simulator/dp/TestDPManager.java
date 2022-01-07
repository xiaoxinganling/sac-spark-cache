package simulator.dp;

import entity.Job;
import entity.RDD;
import entity.Stage;
import org.junit.jupiter.api.Test;
import simulator.HotDataGenerator;
import simulator.JobGenerator;
import sketch.StaticSketch;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class TestDPManager {

    String fileName = "E:\\Google Chrome Download\\";
    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;

    @Test
    void testChoose() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
//            if (!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
            List<Job> jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(fileName + applicationPath[i]);
            List<RDD> hotRDD = HotDataGenerator.hotRDD(applicationName[i], jobList);
            for (RDD rdd : hotRDD) {
                System.out.print(rdd.rddId + "=" + rdd.partitionNum + " ");
            }
            System.out.println();
            Set<Long> choseRDDIds = new HashSet<>();
            Map<Long, Stage> keyStages = KeyPathManager.generateKeyStages(jobList);
            Set<Long> hotRDDIds = new HashSet<>();
            for (RDD rdd : hotRDD) {
                hotRDDIds.add(rdd.rddId);
            }
            Map<Long, Double> cachedTime = RDDTimeManager.cachedRDDTimeWithKeyStages(keyStages, hotRDDIds);
            double maxValue = DPManager.chooseRDDToCache(keyStages, hotRDD, 20, choseRDDIds);
            System.out.println(maxValue + " -> " + choseRDDIds + " size: " + choseRDDIds.size());
            if (choseRDDIds.size() == 1) {
                double max = 0;
                for (int j = 0; j < hotRDDIds.size(); j++) {
                    if (hotRDD.get(j).partitionNum <= 20L) {
                        max = Math.max(max, cachedTime.getOrDefault(hotRDD.get(j).rddId, 0.0));
                    }
                }
                assertEquals(max, maxValue);
            } else if (choseRDDIds.size() == 2) {
                double max = 0;
                for (int j = 0; j < hotRDDIds.size(); j++) {
                    for (int k = j + 1; k < hotRDDIds.size(); k++) {
                        if (hotRDD.get(j).partitionNum + hotRDD.get(k).partitionNum <= 20L) {
                            max = Math.max(max, cachedTime.getOrDefault(hotRDD.get(j).rddId, 0.0) +
                                    cachedTime.getOrDefault(hotRDD.get(k).rddId, 0.0));
                        }
                    }
                }
                assertEquals(max, maxValue);
            } else if (choseRDDIds.size() == 3) {
                double max = 0;
                for (int j = 0; j < hotRDDIds.size(); j++) {
                    for (int k = j + 1; k < hotRDDIds.size(); k++) {
                        for (int p = k + 1; p < hotRDDIds.size(); p++) {
                            if (hotRDD.get(j).partitionNum + hotRDD.get(k).partitionNum + hotRDD.get(p).partitionNum <= 20L) {
                                max = Math.max(max, cachedTime.getOrDefault(hotRDD.get(j).rddId, 0.0) +
                                        cachedTime.getOrDefault(hotRDD.get(k).rddId, 0.0)) +
                                        cachedTime.getOrDefault(hotRDD.get(p).rddId, 0.0);
                            }
                        }
                    }
                }
                assertEquals(max, maxValue);
            }
        }
    }
}