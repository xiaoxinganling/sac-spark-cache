package simulator;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import utils.NumberUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

class TestSimulatorProcess {

    String fileName = "E:\\Google Chrome Download\\";
    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;

//    String[] applicationName = {StaticSketch.applicationName[16]};
//    String[] applicationPath = {StaticSketch.applicationPath[16]};

    // 使用工具人svm进行测试——思考如何高效测试
    // job_id, stage_id, hot_rdd_id, before_cache_space, after_cache_space, values(如果有的话,如lrc和mrd)
//    String[] applicationPath = {StaticSketch.applicationPath[5]};
//    String[] applicationName = {StaticSketch.applicationName[5]}; // svm 5, triangle count 10, kmeans 12, decision_tree 14, pca 15
    Logger logger = Logger.getLogger(TestSimulatorProcess.class);

    @BeforeEach
    void init() {
        for(int i = 0; i < applicationPath.length; i++) {
            applicationPath[i] = fileName + applicationPath[i] ;
        }
    }

    @Test
    void testProcessWithNoCache() {
//        String[] fileNames = {fileName + StaticSketch.applicationPath[6]};
//        String[] applicationNames = {applicationName[6]}; // svm 5
//        SimulatorProcess.processWithNoCache(applicationNames, fileNames);
        SimulatorProcess.processWithNoCache(applicationName, applicationPath);
    }

    @Test
    void testProcessWithInitialCache() {
//        String[] fileNames = {fileName + StaticSketch.applicationPath[5]};
//        String[] applicationNames = {applicationName[5]}; // svm 5
//        SimulatorProcess.processWithInitialCache(applicationNames, fileNames);
        SimulatorProcess.processWithInitialCache(applicationName, applicationPath);
    }

    @Test
    void testProcessWithRuntimeCacheIdeal() {
//        String[] fileNames = {fileName + StaticSketch.applicationPath[15]};
//        String[] applicationNames = {applicationName[15]}; // svm 5, triangle count 10, kmeans 12, decision_tree 14, pca 15
//        SimulatorProcess.processWithRuntimeCache(applicationNames, fileNames);
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.FIFO, 1000000, 4);
    }

    // FIFO
    @Test
    void testProcessWithRuntimeCacheFIFO() {
//        String[] fileNames = {fileName + StaticSketch.applicationPath[15]};
//        String[] applicationNames = {applicationName[15]}; // svm 5, triangle count 10, kmeans 12, decision_tree 14, pca 15
//        SimulatorProcess.processWithRuntimeCache(applicationNames, fileNames);
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.FIFO, 20, 4);
    }

    // LRU
    @Test
    void testProcessWithRuntimeLRU() {
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.LRU, 20, 4);
    }

    // LFU
    @Test
    void testProcessWithRuntimeLFU() {
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.LFU, 20, 4);
    }

    // LRC
    @Test
    void testProcessWithRuntimeLRC() {
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.LRC, 20, 4);
    }

    // MRD
    @Test
    void testProcessWithRuntimeMRD() {
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.MRD, 20, 4);
    }

    // DP
    @Test
    void testProcessWithRuntimeDP() {
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.DP, 20, 4);
    }

    @Test
    void writeExpMetrics() throws IOException {
        ReplacePolicy[] replacePolicies = {ReplacePolicy.FIFO, ReplacePolicy.FIFO, ReplacePolicy.LRU, ReplacePolicy.LFU, ReplacePolicy.LRC, ReplacePolicy.MRD, ReplacePolicy.DP};
        int cacheSize = 20;
        int runnerSize = 4;
        int[] cacheSpaceSize = new int[7];
        cacheSpaceSize[0] = 1000000;
        for (int i = 1; i < 7; i++) {
            cacheSpaceSize[i] = cacheSize;
        }
        String runTimePath = "a_exp_run_time.csv";
        String hitRatioPath = "a_exp_hit_ratio.csv";
        SimulatorProcess.writeExpStatistics(applicationName, applicationPath, replacePolicies, cacheSpaceSize, runTimePath, hitRatioPath, runnerSize, false);
    }

    @Test
    void writeSeparateMetrics() throws IOException {
        double[] cacheSpaceRatio = {0.1, 0.2, 0.3};
        double[] parallelismRatio = {0.5, 1};
        ReplacePolicy[] replacePolicies = {ReplacePolicy.FIFO, ReplacePolicy.LRU, ReplacePolicy.LFU, ReplacePolicy.LRC, ReplacePolicy.MRD, ReplacePolicy.DP};
        String runTimePath = "exp/a_run_time.csv"; // TODO: remove test
        String hitRatioPath = "exp/a_hit_ratio.csv";
        SimulatorProcess.writeExpStatisticsBatch(cacheSpaceRatio, parallelismRatio, replacePolicies,
                runTimePath, hitRatioPath, applicationName, applicationPath);
    }

    @Test
    void writeSeparateMetricsForAll() throws IOException {
        double[] cacheSpaceRatio = new double[10];
        double start = 0.1;
        for (int i = 0; i < cacheSpaceRatio.length; i++) {
            cacheSpaceRatio[i] = start;
            start += 0.1;
        }
        start = 0.1;
        for (double d : cacheSpaceRatio) {
            assert start == d;
            start += 0.1;
        }
        double[] parallelismRatio = {0.5, 1};
        ReplacePolicy[] replacePolicies = {ReplacePolicy.FIFO, ReplacePolicy.LRU, ReplacePolicy.LFU, ReplacePolicy.LRC, ReplacePolicy.MRD, ReplacePolicy.DP};
        String runTimePath = "exp/all/a_run_time.csv";
        String hitRatioPath = "exp/all/a_hit_ratio.csv"; // all
        SimulatorProcess.writeExpStatisticsBatch(cacheSpaceRatio, parallelismRatio, replacePolicies,
                runTimePath, hitRatioPath, applicationName, applicationPath);
    }


    @Test
    void writeSeparateMetricsWithOutFIFO() throws IOException {
        double[] cacheSpaceRatio = new double[10];
        double start = 0.1;
        for (int i = 0; i < cacheSpaceRatio.length; i++) {
            cacheSpaceRatio[i] = start;
            start += 0.1;
        }
        start = 0.1;
        for (double d : cacheSpaceRatio) {
            assert start == d;
            start += 0.1;
        }
        double[] parallelismRatio = {0.5, 1};
        ReplacePolicy[] replacePolicies = {ReplacePolicy.LRU, ReplacePolicy.LFU, ReplacePolicy.LRC, ReplacePolicy.MRD, ReplacePolicy.DP};
//        String runTimePath = "exp/topo/better_dp/a_run_time.csv";
//        String hitRatioPath = "exp/topo/better_dp/a_hit_ratio.csv"; // all TODO: need to change back
        String runTimePath = "exp/topo/better_dp/contrast/a_run_time.csv";
        String hitRatioPath = "exp/topo/better_dp/contrast/a_hit_ratio.csv"; // all
        SimulatorProcess.writeExpStatisticsBatch(cacheSpaceRatio, parallelismRatio, replacePolicies,
                runTimePath, hitRatioPath, applicationName, applicationPath);
    }


    @Test
    void writeIdealMetrics() throws IOException {
        double[] cacheSpaceRatio = {10000.0};
        double[] parallelismRatio = {20};
        ReplacePolicy[] replacePolicies = {ReplacePolicy.FIFO};
        String runTimePath = "exp/ideal/a_run_time.csv";
        String hitRatioPath = "exp/ideal/a_hit_ratio.csv";
        SimulatorProcess.writeExpStatisticsBatch(cacheSpaceRatio, parallelismRatio, replacePolicies,
                runTimePath, hitRatioPath, applicationName, applicationPath);
    }

    @Test
    void writeSeparateMetricsOnlySCC() throws IOException {
        double[] cacheSpaceRatio = new double[10];
        double start = 0.1;
        for (int i = 0; i < cacheSpaceRatio.length; i++) {
            cacheSpaceRatio[i] = start;
            start += 0.1;
        }
        start = 0.1;
        for (double d : cacheSpaceRatio) {
            assert start == d;
            start += 0.1;
        }
        double[] parallelismRatio = {0.25, 0.5, 0.75, 1};
        ReplacePolicy[] replacePolicies = {ReplacePolicy.LRU, ReplacePolicy.LFU, ReplacePolicy.LRC, ReplacePolicy.MRD, ReplacePolicy.DP};
        String runTimePath = "exp/topo/better_dp/only_scc/a_run_time.csv";
        String hitRatioPath = "exp/topo/better_dp/only_scc/a_hit_ratio.csv"; // all
        SimulatorProcess.writeExpStatisticsBatch(cacheSpaceRatio, parallelismRatio, replacePolicies,
                runTimePath, hitRatioPath, applicationName, applicationPath);
    }

    @Test
    void writeSeparateMetricsWithOutFIFOAndHotData() throws IOException {
        double[] cacheSpaceRatio = new double[10];
        double start = 0.1;
        for (int i = 0; i < cacheSpaceRatio.length; i++) {
            cacheSpaceRatio[i] = start;
            start += 0.1;
        }
        start = 0.1;
        for (double d : cacheSpaceRatio) {
            assert start == d;
            start += 0.1;
        }
        double[] parallelismRatio = {0.5, 1};
        ReplacePolicy[] replacePolicies = {ReplacePolicy.LRU, ReplacePolicy.LFU, ReplacePolicy.LRC, ReplacePolicy.MRD, ReplacePolicy.DP};
        String runTimePath = "exp/topo/better_dp/without_hot_data/a_run_time.csv";
        String hitRatioPath = "exp/topo/better_dp/without_hot_data/a_hit_ratio.csv"; // all
        SimulatorProcess.writeExpStatisticsBatch(cacheSpaceRatio, parallelismRatio, replacePolicies,
                runTimePath, hitRatioPath, applicationName, applicationPath);

    }

    @Test
    void writeSeparateMetricsWithSchedule() throws IOException {
        double[] cacheSpaceRatio = new double[10];
        double start = 0.1;
        for (int i = 0; i < cacheSpaceRatio.length; i++) {
            cacheSpaceRatio[i] = start;
            start += 0.1;
        }
        start = 0.1;
        for (double d : cacheSpaceRatio) {
            assert start == d;
            start += 0.1;
        }
        double[] parallelismRatio = {0.5, 1};
        ReplacePolicy[] replacePolicies = {ReplacePolicy.LRU, ReplacePolicy.LFU, ReplacePolicy.LRC, ReplacePolicy.MRD, ReplacePolicy.DP};
        String runTimePath = "exp/topo/better_dp/schedule/a_run_time.csv";
        String hitRatioPath = "exp/topo/better_dp/schedule/a_hit_ratio.csv"; // all
        SimulatorProcess.writeExpStatisticsBatch(cacheSpaceRatio, parallelismRatio, replacePolicies,
                runTimePath, hitRatioPath, applicationName, applicationPath);
    }

}