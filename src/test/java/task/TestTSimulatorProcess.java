package task;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import simulator.ReplacePolicy;
import sketch.StaticSketch;
import task.scheduler.FIFOScheduler;

import java.io.IOException;

class TestTSimulatorProcess {

    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = new String[StaticSketch.applicationPath.length];
    String fileName = "E:\\Google Chrome Download\\";

    @BeforeEach
    void init() {
        for (int i = 0; i < applicationPath.length; i++) {
            applicationPath[i] = fileName + StaticSketch.applicationPath[i];
        }
    }

    @Test
    void testProcessWithFIFOScheduleAndNoCache() throws IOException {
        TSimulatorProcess.processWithFIFOScheduleAndNoCache(applicationName, applicationPath);
    }

    @Test
    void testProcessWithFIFOScheduleAndSLRUCacheZeroSpace() throws IOException {
        // zero size
        TSimulatorProcess.processWithFIFOScheduleAndCache(applicationName, applicationPath,
                ReplacePolicy.SLRU, 0, 100, 0.2);
    }

    @Deprecated
    @Test
    void testProcessWithFIFOScheduleAndSLRUCache20Space() throws IOException {
        // 20 size
        TSimulatorProcess.processWithFIFOScheduleAndCache(applicationName, applicationPath,
                ReplacePolicy.SLRU, 40, 100, 0.2);
    }

    @Test
    void testProcessWithFIFOScheduleAndSLRUCacheWithInfSpace() throws IOException {
        // infinite size
        TSimulatorProcess.processWithFIFOScheduleAndCache(applicationName, applicationPath,
                ReplacePolicy.SLRU, SCacheSpace.INF_SPACE, 100, 1);
    }

    @Test
    void testProcessWithSimpleFIFOScheduleAndSLRUCacheWithRatioOfCacheSpace() throws IOException {
        // infinite size
        TSimulatorProcess.processWithFIFOScheduleAndCache(applicationName, applicationPath,
                ReplacePolicy.SLRU, SCacheSpace.INF_SPACE, 100, 0.2);
    }

    @Test
    void testProcessWithFIFOScheduleAndLRU() throws IOException {
        TSimulatorProcess.processWithNewScheduleAndCache(applicationName, applicationPath,
                ReplacePolicy.SLRU, 40, 0.2, ScheduleType.FIFO);
    }

    @Test
    void testProcessWithDAGAwareScheduleAndLRU() throws IOException {
        TSimulatorProcess.processWithNewScheduleAndCache(applicationName, applicationPath,
                ReplacePolicy.SLRU, 40, 0.2, ScheduleType.DAGAware);
    }

    @Test
    void testProcessWithFIFOScheduleAndLRC() throws IOException {
        TSimulatorProcess.processWithNewScheduleAndCache(applicationName, applicationPath,
                ReplacePolicy.SLRC, 40, 1, ScheduleType.FIFO);
    }

    @Test
    void testProcessWithDAGAwareScheduleAndLRC() throws IOException {
        TSimulatorProcess.processWithNewScheduleAndCache(applicationName, applicationPath,
                ReplacePolicy.SLRC, 40, 0.2, ScheduleType.DAGAware);
    }

    @Test
    void testProcessWithFIFOScheduleAndMRD() throws IOException {
        TSimulatorProcess.processWithNewScheduleAndCache(applicationName, applicationPath,
                ReplacePolicy.SMRD, 40, 0.2, ScheduleType.FIFO);
    }

    @Test
    void testProcessWithDAGAwareScheduleAndMRD() throws IOException {
        TSimulatorProcess.processWithNewScheduleAndCache(applicationName, applicationPath,
                ReplacePolicy.SMRD, 40, 0.2, ScheduleType.DAGAware);
    }

}