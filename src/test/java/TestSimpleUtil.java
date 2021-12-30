import entity.RDD;
import entity.Stage;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import sun.rmi.runtime.Log;
import utils.CacheSketcher;
import utils.ResultOutputer;
import utils.SimpleUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class TestSimpleUtil {


    String fileName = "E:\\Google Chrome Download\\";
    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    Logger logger = Logger.getLogger(TestSimpleUtil.class);

    @Test
    void testComputeTimeOfStage() throws IOException {
        Set<Long> choseStages = new HashSet<>();
        choseStages.add(9L);
        choseStages.add(10L);
        choseStages.add(11L);
        choseStages.add(12L);
        for(int i = 0; i < applicationName.length; i++) {
            System.out.println("test application's : " + applicationName[i] + " stage compute time");
            if(!applicationName[i].contains("spark_svm")) {
                continue;
            }
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
            Map<Long, Stage> stageMap = new HashMap<>();
            for(StageCompletedEvent sce : stageList) {
                if(choseStages.contains(sce.stage.stageId)) {
                    stageMap.put(sce.stage.stageId, sce.stage);
                }
            }
            System.out.println(SimpleUtil.computeTimeOfStage(stageMap.get(12L), stageMap));
            assertEquals(11, SimpleUtil.computeTimeOfStage(stageMap.get(12L), stageMap));
        }
    }

    @Test
    void testJobsContainsParallelStages() throws IOException {
        for(int i = 0; i < applicationName.length; i++) {
            System.out.println("test application's : " + applicationName[i] + " jobs with parallel stages");
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
            List<Long> isParallelJobs = new ArrayList<>();
            for(JobStartEvent job : jobList) {
                boolean isParallel = SimpleUtil.jobContainsParallelStages(job, stageList);
                if(isParallel) {
//                    System.out.println("application_id: " + applicationName[i] + ", job id: " + job.jobId + ": true");
                    isParallelJobs.add(job.jobId);
                }
            }
            System.out.println(isParallelJobs);
        }
    }

    @Test
    void testRDDToCacheInApplication() throws IOException {
        for(int i = 0; i < applicationName.length; i++) {
            System.out.println("test application's : " + applicationName[i] + " rdd to cache");
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
            List<Long> toCacheInApplication = SimpleUtil.rddToCacheInApplication(jobList, stageList);
            System.out.println(toCacheInApplication + " " + toCacheInApplication.size());
        }
    }

    @Test
    void testRDDToCacheInJob() throws IOException {
        for(int i = 0; i < applicationName.length; i++) {
            System.out.println("test application's : " + applicationName[i] + " jobs' rdd to cache");
            if(!applicationName[i].contains("spark_svm")) {
                continue;
            }
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
            List<Long> toCacheInApplication = SimpleUtil.rddToCacheInApplication(jobList, stageList);
            for(JobStartEvent job : jobList) {
                if(job.jobId.equals(6L)) {
                    List<Long> toCacheInJob = SimpleUtil.rddToCacheInJob(toCacheInApplication, job, stageList);
                    System.out.println(toCacheInJob + " " + toCacheInJob.size());
                }
            }
        }
    }

    @Test
    void testComputeTimeOfStageWithAccumulation() throws IOException {
        Set<Long> choseStages = new HashSet<>();
        choseStages.add(9L);
        choseStages.add(10L);
        choseStages.add(11L);
        choseStages.add(12L);
        for(int i = 0; i < applicationName.length; i++) {
            System.out.println("test application's : " + applicationName[i] + " stage compute time");
            if(!applicationName[i].contains("spark_svm")) {
                continue;
            }
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
            Map<Long, Stage> stageMap = new HashMap<>();
            for(StageCompletedEvent sce : stageList) {
                if(choseStages.contains(sce.stage.stageId)) {
                    stageMap.put(sce.stage.stageId, sce.stage);
                }
            }
            int time = SimpleUtil.computeTimeOfStageWithAccumulation(stageMap.get(12L), stageMap);
            System.out.println(time);
            assertEquals(15, time);
        }
    }

    @Test
    void testComputeTimeWithShortestPath() throws IOException {
        Set<Long> choseStages = new HashSet<>();
        choseStages.add(9L);
        choseStages.add(10L);
        choseStages.add(11L);
        choseStages.add(12L);
        for(int i = 0; i < applicationName.length; i++) {
            System.out.println("test application's : " + applicationName[i] + " stage compute time");
            if(!applicationName[i].contains("spark_svm")) {
                continue;
            }
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
            Map<Long, Stage> stageMap = new HashMap<>();
            for(StageCompletedEvent sce : stageList) {
                if(choseStages.contains(sce.stage.stageId)) {
                    stageMap.put(sce.stage.stageId, sce.stage);
                }
            }
            int computeTime = SimpleUtil.computeTimeOfStageWithShortestPath(stageMap.get(12L), stageMap);
            System.out.println(computeTime);
            assertEquals(10, computeTime);
        }
    }

    @Test
    void testComputeTimeOfStageWithCache() throws IOException {
        {
            Set<Long> choseStages = new HashSet<>();
            choseStages.add(9L);
            choseStages.add(10L);
            choseStages.add(11L);
            choseStages.add(12L);
            for(int i = 0; i < applicationName.length; i++) {
                System.out.println("test application's : " + applicationName[i] + " stage compute time");
                if(!applicationName[i].contains("spark_svm")) {
                    continue;
                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
                Map<Long, Stage> stageMap = new HashMap<>();
                for(StageCompletedEvent sce : stageList) {
                    if(choseStages.contains(sce.stage.stageId)) {
                        stageMap.put(sce.stage.stageId, sce.stage);
                    }
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(2L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCache(choseRDD, stageMap.get(12L), stageMap);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(8, computeTime);
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(3L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCache(choseRDD, stageMap.get(12L), stageMap);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(10, computeTime);
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(3L);
                    choseRDD.add(2L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCache(choseRDD, stageMap.get(12L), stageMap);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(7, computeTime);
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(3L);
                    choseRDD.add(2L);
                    choseRDD.add(27L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCache(choseRDD, stageMap.get(12L), stageMap);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(6, computeTime);
                }
            }
        }
    }

    @Test
    void testComputeTimeOfStageWithCacheAndAccumulation() throws IOException {
        {
            Set<Long> choseStages = new HashSet<>();
            choseStages.add(9L);
            choseStages.add(10L);
            choseStages.add(11L);
            choseStages.add(12L);
            for(int i = 0; i < applicationName.length; i++) {
                System.out.println("test application's : " + applicationName[i] + " stage compute time");
                if(!applicationName[i].contains("spark_svm")) {
                    continue;
                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
                Map<Long, Stage> stageMap = new HashMap<>();
                for(StageCompletedEvent sce : stageList) {
                    if(choseStages.contains(sce.stage.stageId)) {
                        stageMap.put(sce.stage.stageId, sce.stage);
                    }
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(2L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCacheAndAccumulation(choseRDD, stageMap.get(12L), stageMap);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(9, computeTime);
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(3L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCacheAndAccumulation(choseRDD, stageMap.get(12L), stageMap);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(11, computeTime);
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(3L);
                    choseRDD.add(2L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCacheAndAccumulation(choseRDD, stageMap.get(12L), stageMap);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(8, computeTime);
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(3L);
                    choseRDD.add(2L);
                    choseRDD.add(27L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCacheAndAccumulation(choseRDD, stageMap.get(12L), stageMap);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(7, computeTime);
                }
            }
        }
    }

    @Test
    void testComputeTimeOfStageWithCacheByLPath() throws IOException {
        {
            Set<Long> choseStages = new HashSet<>();
            choseStages.add(9L);
            choseStages.add(10L);
            choseStages.add(11L);
            choseStages.add(12L);
            for(int i = 0; i < applicationName.length; i++) {
                System.out.println("test application's : " + applicationName[i] + " stage compute time");
                if(!applicationName[i].contains("spark_svm")) {
                    continue;
                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
                Map<Long, Stage> stageMap = new HashMap<>();
                for(StageCompletedEvent sce : stageList) {
                    if(choseStages.contains(sce.stage.stageId)) {
                        stageMap.put(sce.stage.stageId, sce.stage);
                    }
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(2L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCacheByLSPath(choseRDD, stageMap.get(12L), stageMap, true);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(8, computeTime);
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(3L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCacheByLSPath(choseRDD, stageMap.get(12L), stageMap, true);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(7, computeTime);
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(3L);
                    choseRDD.add(2L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCacheByLSPath(choseRDD, stageMap.get(12L), stageMap, true);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(7, computeTime);
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(3L);
                    choseRDD.add(2L);
                    choseRDD.add(27L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCacheByLSPath(choseRDD, stageMap.get(12L), stageMap, true);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(6, computeTime);
                }
            }
        }
    }

    @Test
    void testComputeTimeOfStageWithCacheBySPath() throws IOException {
        // TODO:
        {
            Set<Long> choseStages = new HashSet<>();
            choseStages.add(9L);
            choseStages.add(10L);
            choseStages.add(11L);
            choseStages.add(12L);
            for(int i = 0; i < applicationName.length; i++) {
                System.out.println("test application's : " + applicationName[i] + " stage compute time");
                if(!applicationName[i].contains("spark_svm")) {
                    continue;
                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
                Map<Long, Stage> stageMap = new HashMap<>();
                for(StageCompletedEvent sce : stageList) {
                    if(choseStages.contains(sce.stage.stageId)) {
                        stageMap.put(sce.stage.stageId, sce.stage);
                    }
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(2L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCacheByLSPath(choseRDD, stageMap.get(12L), stageMap, false);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(7, computeTime);
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(3L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCacheByLSPath(choseRDD, stageMap.get(12L), stageMap, false);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(10, computeTime);
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(3L);
                    choseRDD.add(2L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCacheByLSPath(choseRDD, stageMap.get(12L), stageMap, false);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(7, computeTime);
                }
                {
                    Set<Long> choseRDD = new HashSet<>();
                    choseRDD.add(3L);
                    choseRDD.add(2L);
                    choseRDD.add(27L);
                    int computeTime = SimpleUtil.computeTimeOfStageWithCacheByLSPath(choseRDD, stageMap.get(12L), stageMap, false);
                    System.out.println(choseRDD + " " + computeTime);
                    assertEquals(6, computeTime);
                }
            }
        }
    }

    @Test
    void testGenerateLastStageOfJob() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
            System.out.println("test application's : " + applicationName[i] + " stage compute time");
            if (!applicationName[i].contains("spark_svm")) {
                continue;
            }
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
            for(JobStartEvent job : jobList) {
                if(job.jobId == 6L) {
                    Stage s = SimpleUtil.lastStageOfJob(job);
                    System.out.println(job.jobId + " " + s.stageId);
                    assertEquals(12L, s.stageId);
                }else if(job.jobId == 8L) {
                    Stage s = SimpleUtil.lastStageOfJob(job);
                    System.out.println(job.jobId + " " + s.stageId);
                    assertEquals(22L, s.stageId);
                }else if(job.jobId == 7L) {
                    Stage s = SimpleUtil.lastStageOfJob(job);
                    System.out.println(job.jobId + " " + s.stageId);
                    assertEquals(17L, s.stageId);
                }
            }
        }
    }

    @Test
    void testStageMapOfJob() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
            System.out.println("test application's : " + applicationName[i] + " stage compute time");
            if (!applicationName[i].contains("spark_svm")) {
                continue;
            }
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
            for (JobStartEvent job : jobList) {
                if (job.jobId == 6L) {
                    Map<Long, Stage> stageMap = SimpleUtil.stageMapOfJob(job);
                    System.out.println(stageMap.keySet());
                    assertEquals(4, stageMap.size());
                }
            }
        }
    }

    @Test
    void testGenerateDifferenceRatio() {
        {
            int a = 0;
            int b = 0;
            double ratio = SimpleUtil.generateDifferenceRatio(a, b);
            System.out.println(a + " " + b + " " + ratio);
            assertEquals(0, ratio);
        }
        {
            int a = 100;
            int b =103;
            double ratio = SimpleUtil.generateDifferenceRatio(a, b);
            System.out.println(a + " " + b + " " + ratio);
            assertEquals(0.03, ratio);
        }
        {
            int a = 0;
            int b = 5;
            double ratio = SimpleUtil.generateDifferenceRatio(a, b);
            System.out.println(a + " " + b + " " + ratio);
            assertEquals(Double.MAX_VALUE, ratio);
        }
    }

    @Test
    void testStageContainsParallelComputation() throws IOException {
        {
            // KEYPOINT: very long time!!!!!!
            if(new File("stage_contains_parallel_info").exists()) {
                return;
            }
            BufferedWriter bw = new BufferedWriter(new FileWriter("stage_contains_parallel_info", true));
            int totalNum = 0;
            int noParallelNum = 0;
            for (int i = 0; i < applicationName.length; i++) {
                int appTotalNum = 0;
                int appNoParallelNum = 0;
//            System.out.println("test application's : " + applicationName[i] + " stage compute time");
//            if (!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
                Set<Long> actualStage = new HashSet<>();
                for(StageCompletedEvent sce : stageList) {
                    actualStage.add(sce.stage.stageId);
                }
                int[][] simpleDAG = CacheSketcher.generateSimpleDAGByJobsAndStages(jobList, stageList);
                int curDealStageSize = 0;
                for(JobStartEvent jse : jobList) {
//                Map<Long, Stage> stageMap = SimpleUtil.filteredStageMapOfJob(jse, stageList);
                    for(Stage stage : jse.stages) {
                        if(!actualStage.contains(stage.stageId)) {
                            continue;
                        }
                        boolean containsParallel = SimpleUtil.stageContainsParallelComputation(jse, stage, simpleDAG, jobList.size(), bw);
//                    System.out.println(totalNum + " -> job " + jse.jobId + " stage " + stage.stageId + " contains parallel: " + containsParallel);
                        if(!containsParallel) {
                            noParallelNum++;
                            appNoParallelNum++;
                        }
                        appTotalNum++;
                        totalNum++;
                        System.out.println("process: " + curDealStageSize++ + "/" + stageList.size());
                    }
                }
                System.out.println(applicationName[i] + " -> totalNum: " + appTotalNum + " noParallelNum: " + appNoParallelNum);
                bw.write(applicationName[i] + " -> totalNum: " + appTotalNum + " noParallelNum: " + appNoParallelNum + "\n");
            }
            System.out.println("totalNum: " + totalNum + " noParallelNum: " + noParallelNum);
            bw.write("totalNum: " + totalNum + " noParallelNum: " + noParallelNum + "\n");
            bw.close();
        }
    }

    @Test
    void testStageContainsParallelComputationInitial() throws IOException {
        {
            if(new File("stage_contains_parallel_info_initial").exists()) {
                return;
            }
            BufferedWriter bw = new BufferedWriter(new FileWriter("stage_contains_parallel_info_initial", true));
            int totalNum = 0;
            int noParallelNum = 0;
            for (int i = 0; i < applicationName.length; i++) {
                int appTotalNum = 0;
                int appNoParallelNum = 0;
//            System.out.println("test application's : " + applicationName[i] + " stage compute time");
//            if (!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
                Set<Long> actualStage = new HashSet<>();
                for(StageCompletedEvent sce : stageList) {
                    actualStage.add(sce.stage.stageId);
                }
                int curDealStageSize = 0;
                for(JobStartEvent jse : jobList) {
//                Map<Long, Stage> stageMap = SimpleUtil.filteredStageMapOfJob(jse, stageList);
                    for(Stage stage : jse.stages) {
                        if(!actualStage.contains(stage.stageId)) {
                            continue;
                        }
                        boolean containsParallel = SimpleUtil.stageContainsParallelComputationInitial(stage);
                        if(containsParallel) {
                            System.out.println(totalNum + " -> job " + jse.jobId + " stage " + stage.stageId + " contains parallel: " + true);
                            bw.write(applicationName[i] + " job " + jse.jobId + " stage " + stage.stageId + " contains parallel.\n");
                        }
                        if(!containsParallel) {
                            noParallelNum++;
                            appNoParallelNum++;
                        }
                        appTotalNum++;
                        totalNum++;
                        System.out.println("process: " + curDealStageSize++ + "/" + stageList.size());
                    }
                }
                System.out.println(applicationName[i] + " -> totalNum: " + appTotalNum + " noParallelNum: " + appNoParallelNum);
                bw.write(applicationName[i] + " -> totalNum: " + appTotalNum + " noParallelNum: " + appNoParallelNum + "\n");
            }
            System.out.println("totalNum: " + totalNum + " noParallelNum: " + noParallelNum);
            bw.write("totalNum: " + totalNum + " noParallelNum: " + noParallelNum + "\n");
            bw.close();
        }

    }

    @Test
    void testRepresentTimeOfCachedRDDAmongJobsAndStages() throws IOException {
        {
            for (int i = 0; i < applicationName.length; i++) {
                BufferedWriter bw = new BufferedWriter(new FileWriter("all_represent_time_depth", true));
                bw.write(applicationName[i] + "\n");
//                if (!applicationName[i].contains("spark_svm")) {
//                    continue;
//                }
//                if(applicationName[i].contains("spark_strongly")) {
//                    continue;
//                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
                List<Long> allToCache = SimpleUtil.rddToCacheInApplication(jobList, stageList);
                // index from 0 to len-1
                Set<Long> actualStage = new HashSet<>();
                for(StageCompletedEvent sce : stageList) {
                    actualStage.add(sce.stage.stageId);
                }
                int curJob = 0;
                for(int j = 0; j < jobList.size(); j++) {
                    // [j, jobList.size() - 1]
                    System.out.println(applicationName[i] + ": " + ++curJob + "/" + jobList.size());
                    List<JobStartEvent> newJobList = jobList.subList(j, jobList.size());
                    List<Stage> newStageList = new ArrayList<>();
                    List<Long> rddToCache = new ArrayList<>();
                    for(int k = j; k < jobList.size(); k++) {
                        for(Stage stage : jobList.get(k).stages) {
                            if(actualStage.contains(stage.stageId)) {
                                newStageList.add(stage);
                                for(RDD rdd : stage.rdds) {
                                    if(allToCache.contains(rdd.rddId)) {
                                        if(!rddToCache.contains(rdd.rddId)) {
                                            rddToCache.add(rdd.rddId);
                                        } //KEYPOINT: 时刻要考虑去重啊
                                    }
                                }
                            }
                        }
                    }
                    Map<Long, List<Double>> representTime = SimpleUtil.representTimeOfCachedRDDAmongJobsAndStages(newStageList, rddToCache);
                    for(List<Double> list : representTime.values()) {
                        for(int k = 0; k < list.size() - 1; k++) {
                            if(!list.get(k).equals(list.get(k + 1))) {
                                System.out.println("!!!!!!!!! " + applicationName[i] + " " + list); // 双重检测
                                bw.write("!!!!!!!!! " + applicationName[i] + " " + list + "\n");
                            }
                        }
                    }
                    ResultOutputer.writeHashMap(bw, representTime);
                }
                bw.close();
            }
        }
    }

    @Test
    void testLastRDDofStage() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
            if(!applicationName[i].contains("spark_strongly")) {
                continue;
            }
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
            logger.info("test stage of " + applicationName[i]);
            int curStage = 0;
            for(StageCompletedEvent sce : stageList) {
                logger.info(++curStage + "/" + stageList.size() + ": " + sce.stage.stageId + "——" +SimpleUtil.lastRDDOfStage(sce.stage).rddId);
            }
        }
    }

    @Test
    void testLastRDDTimeOfStage() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
//            if(!applicationName[i].contains("spark_svm")) {
//                continue;
//            }
            if(!applicationName[i].contains("spark_strongly")) {
                continue;
            }
            List<JobStartEvent> jobList = new ArrayList<>();
            List<StageCompletedEvent> stageList = new ArrayList<>();
            StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
            logger.info("test stage time of " + applicationName[i]);
            int curStage = 0;
            for(StageCompletedEvent sce : stageList) {
                Map<Long, RDD> rddMap = new HashMap<>();
                for(RDD rdd : sce.stage.rdds) {
                    rddMap.put(rdd.rddId, rdd);
                }
                curStage++;
                RDD lastRDD = SimpleUtil.lastRDDOfStage(sce.stage);
//                logger.info(++curStage + "/" + stageList.size() + ": " + sce.stage.stageId + "——" +SimpleUtil.lastRDDOfStage(sce.stage).rddId);
                logger.info("rdd size in Stage——" + sce.stage.stageId + ": " + rddMap.size());
                logger.info(curStage + "/" + stageList.size() + ": " + sce.stage.stageId + "——" + SimpleUtil.lastRDDTimeOfStage(rddMap, lastRDD));
//                logger.info(curStage + "/" + stageList.size() + ": " + sce.stage.stageId + "——" + SimpleUtil.lastRDDTimeOfStageV2(rddMap, lastRDD));
//                double stageTime = SimpleUtil.lastRDDTimeOfStage(rddMap, SimpleUtil.lastRDDOfStage(sce.stage));
//                logger.info(curStage + "/" + stageList.size() + ": " + sce.stage.stageId + "——" + stageTime);
//                logger.debug(curStage + "/" + stageList.size() + ": " + sce.stage.stageId + "——" + stageTime);
            }
        }
    }

}
