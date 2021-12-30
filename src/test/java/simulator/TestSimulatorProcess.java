package simulator;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import static org.junit.jupiter.api.Assertions.*;

class TestSimulatorProcess {

    String fileName = "E:\\Google Chrome Download\\";
    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    Logger logger = Logger.getLogger(TestSimulatorProcess.class);

    @Test
    void testProcessWithNoCache() {
        String[] fileNames = {fileName + applicationPath[5]};
        SimulatorProcess.processWithNoCache(fileNames);
    }

}