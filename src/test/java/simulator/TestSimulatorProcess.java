package simulator;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;

class TestSimulatorProcess {

    String fileName = "E:\\Google Chrome Download\\";
    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = new String[StaticSketch.applicationPath.length];
    Logger logger = Logger.getLogger(TestSimulatorProcess.class);

    @BeforeEach
    void init() {
        for(int i = 0; i < applicationPath.length; i++) {
            applicationPath[i] = fileName + StaticSketch.applicationPath[i];
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
    void testProcessWithRuntimeCache() {
//        String[] fileNames = {fileName + StaticSketch.applicationPath[5]};
//        String[] applicationNames = {applicationName[5]}; // svm 5
//        SimulatorProcess.processWithRuntimeCache(applicationNames, fileNames);
        SimulatorProcess.processWithRuntimeCache(applicationName, applicationPath);
    }

}