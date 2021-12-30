package utils;

import entity.RDD;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class TestCriticalPathUtil {

    String fileName = "E:\\Google Chrome Download\\";
    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;
    Logger logger = Logger.getLogger(TestCriticalPathUtil.class);

    @Test
    void testGetLongestTimeOfStage() throws IOException {
        for (int i = 0; i < applicationName.length; i++) {
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
                logger.info("rdd size in Stage——" + sce.stage.stageId + ": " + rddMap.size());
//                logger.info(curStage + "/" + stageList.size() + ": " + sce.stage.stageId + "——" + SimpleUtil.lastRDDTimeOfStage(rddMap, lastRDD));
                logger.info(curStage + "/" + stageList.size() + ": " + sce.stage.stageId + "——" + CriticalPathUtil.getLongestTimeOfStage(sce.stage));
            }
        }
    }

}