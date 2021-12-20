import entity.sac.CacheCandidate;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;
import entity.sac.CacheCandidateRule;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestCacheCandidates {

    @Test
    void testCacheCandidateEntity() {
        {
            int[] ref = {32, 11, 9, 9, 9, 9, 9, 9, 7, 5, 2, 2, 2, 2, 2, 2, 2, 2};
            int[] directRef = {12, 5, 3, 3, 3, 3, 3, 5, 5, 5, 2, 2, 2, 2, 2, 2, 2, 2};
            int[] hopComputeTime = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
            long[] partitions = {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 4, 4, 4, 4};
            for(int i = 0; i < ref.length; i++) {
                CacheCandidate cc = new CacheCandidate(0L, ref[i], directRef[i], hopComputeTime[i], partitions[i]);
                assertEquals((ref[i] - 1) * (float) Math.log(hopComputeTime[i] / (float) partitions[i] + 1), cc.getCe());
                assertEquals((directRef[i] - 1) * (float) Math.log(hopComputeTime[i] / (float) partitions[i] + 1), cc.getDirectCe());
            }
        }
    }

    @Test
    void testGenerateJobAndStageList() {
        {
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            try {
                StaticSketch.generateJobAndStageList(jobList, stageList, "E:\\Google Chrome Download\\application_1634644457236_0031_1");
            } catch (IOException e) {
                e.printStackTrace();
            }
            assertEquals(4, jobList.size());
            assertEquals(10, stageList.size());
        }
    }

    @Test
    void testGeneratedSortedRawCCByHistory() throws IOException {
        List<List<Long>> contrastParent = new ArrayList<>();
        List<Long> parent0 = new ArrayList<>();
        contrastParent.add(parent0);

        List<Long> parent1 = new ArrayList<>();
        parent1.add(0L);
        contrastParent.add(parent1);

        List<Long> parent2 = new ArrayList<>();
        parent2.add(0L);
        parent2.add(1L);
        contrastParent.add(parent2);

        List<Long> parent3 = new ArrayList<>();
        parent3.add(2L);
        contrastParent.add(parent3);

        List<Long> parent4 = new ArrayList<>();
        parent4.add(3L);
        contrastParent.add(parent4);

        List<Long> parent5 = new ArrayList<>();
        parent5.add(4L);
        contrastParent.add(parent5);

        List<Long> parent6 = new ArrayList<>();
        parent6.add(0L);
        parent6.add(5L);
        contrastParent.add(parent6);

        List<Long> parent7 = new ArrayList<>();
        parent7.add(6L);
        contrastParent.add(parent7);

        List<Long> parent8 = new ArrayList<>();
        parent8.add(7L);
        contrastParent.add(parent8);

        List<Long> parent9 = new ArrayList<>();
        parent9.add(8L);
        contrastParent.add(parent9);

        List<Long> parent10 = new ArrayList<>();
        parent10.add(1L);
        parent10.add(9L);
        contrastParent.add(parent10);

        List<Long> parent11 = new ArrayList<>();
        parent11.add(10L);
        contrastParent.add(parent11);

        List<Long> parent12 = new ArrayList<>();
        parent12.add(11L);
        contrastParent.add(parent12);

        List<Long> parent13 = new ArrayList<>();
        parent13.add(12L);
        contrastParent.add(parent13);

        List<Long> parent14 = new ArrayList<>();
        parent14.add(9L);
        parent14.add(13L);
        contrastParent.add(parent14);

        List<Long> parent15 = new ArrayList<>();
        parent15.add(14L);
        contrastParent.add(parent15);

        List<Long> parent16 = new ArrayList<>();
        parent16.add(15L);
        contrastParent.add(parent16);

        List<Long> parent17 = new ArrayList<>();
        parent17.add(16L);
        contrastParent.add(parent17);

        String fileName = "E:\\Google Chrome Download\\application_1634644457236_0031_1";
        {
            List<CacheCandidate> sortedRawCC = StaticSketch.generateSortedRawCacheCandidatesByHistory(fileName, true);
            assertEquals(18, sortedRawCC.size());
            for(int i = 0; i < sortedRawCC.size() - 1; i++) {
                assertTrue(sortedRawCC.get(i).getDirectCe() <= sortedRawCC.get(i + 1).getDirectCe());
            }
            assertEquals(0, sortedRawCC.get(0).getDirectCe());
            assertEquals("6.818992", String.format("%.6f", sortedRawCC.get(sortedRawCC.size() - 1).getDirectCe()));
            sortedRawCC.sort((x, y) -> (int) (x.rddId - y.rddId));
            for(int i = 0; i < sortedRawCC.size(); i++) {
                System.out.println("assert rdd: " + i);
                List<Long> parents = sortedRawCC.get(i).getParentIds();
                parents.sort((x, y) -> (int) (x - y));
                assertEquals(contrastParent.get(i).size(), parents.size());
                for(int j = 0; j < parents.size(); j++) {
                    System.out.println("assert rdd: " + i + ", parent: " + j + ", value: " + parents.get(j));
                    assertEquals(contrastParent.get(i).get(j), parents.get(j));
                }
            }
        }
        {
            List<CacheCandidate> sortedRawCC = StaticSketch.generateSortedRawCacheCandidatesByHistory(fileName, false);
            assertEquals(18, sortedRawCC.size());
            for(int i = 0; i < sortedRawCC.size() - 1; i++) {
                assertTrue(sortedRawCC.get(i).getCe() <= sortedRawCC.get(i + 1).getCe());
            }
            assertEquals(0, sortedRawCC.get(0).getCe());
            assertEquals("14.334076", String.format("%.6f", sortedRawCC.get(sortedRawCC.size() - 1).getCe()));
            sortedRawCC.sort((x, y) -> (int) (x.rddId - y.rddId));
            for(int i = 0; i < sortedRawCC.size(); i++) {
                System.out.println("assert rdd: " + i);
                List<Long> parents = sortedRawCC.get(i).getParentIds();
                parents.sort((x, y) -> (int) (x - y));
                assertEquals(contrastParent.get(i).size(), parents.size());
                for(int j = 0; j < parents.size(); j++) {
                    System.out.println("assert rdd: " + i + ", parent: " + j + ", value: " + parents.get(j));
                    assertEquals(contrastParent.get(i).get(j), parents.get(j));
                }
            }
        }
    }

    @Test
    void testGenerateTargetCC() throws IOException {
        float[] thresholdArr = new float[10];
        float start = 0f;
        for(int i = 0; i < 10; i++) {
            thresholdArr[i] = start;
            start += 0.1;
        }
        String fileName = "E:\\Google Chrome Download\\application_1634644457236_0031_1";
        {
            boolean isDirect = false;
            float max = Float.MIN_VALUE;
            float min = Float.MAX_VALUE;
            List<CacheCandidate> sortedRawCC = StaticSketch.generateSortedRawCacheCandidatesByHistory(fileName, isDirect);
            float[] targetLen = new float[10];
            for(CacheCandidate cc : sortedRawCC) {
                max = Math.max(max, cc.getCe());
                min = Math.min(min, cc.getCe());
            }
            for(int i = 0; i < thresholdArr.length; i++) {
                float threshold = thresholdArr[i];
                for(CacheCandidate cc : sortedRawCC) {
                    if(cc.getCe() / (max - min) >= threshold) {
                        targetLen[i]++;
                    }
                }
            }
            for(int i = 0; i < thresholdArr.length; i++) {
                System.out.println("asserting target cc with threshold: " + thresholdArr[i]);
                List<CacheCandidate> targetCC = StaticSketch.generateTargetCacheCandidates(sortedRawCC,
                        new CacheCandidateRule(thresholdArr[i], isDirect));
                assertEquals(targetLen[i], targetCC.size());
            }
        }
    }

}
