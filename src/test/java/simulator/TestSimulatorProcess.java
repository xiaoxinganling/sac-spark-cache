package simulator;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;

class TestSimulatorProcess {

    String fileName = "E:\\Google Chrome Download\\";
    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;

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
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.FIFO, 1000000);
    }

    // FIFO
    @Test
    void testProcessWithRuntimeCacheFIFO() {
//        String[] fileNames = {fileName + StaticSketch.applicationPath[15]};
//        String[] applicationNames = {applicationName[15]}; // svm 5, triangle count 10, kmeans 12, decision_tree 14, pca 15
//        SimulatorProcess.processWithRuntimeCache(applicationNames, fileNames);
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.FIFO, 20);
    }

    // LRU
    @Test
    void testProcessWithRuntimeLRU() {
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.LRU, 20);
    }

    // LFU
    @Test
    void testProcessWithRuntimeLFU() {
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.LFU, 20);
    }

    // LRC
    @Test
    void testProcessWithRuntimeLRC() {
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.LRC, 20);
    }

    // MRD
    @Test
    void testProcessWithRuntimeMRD() {
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.MRD, 20);
    }

    // DP
    @Test
    void testProcessWithRuntimeDP() {
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath, ReplacePolicy.DP, 20);
    }

}