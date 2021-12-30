package simulator;

import entity.Stage;
import org.apache.log4j.Logger;

import java.util.List;

public class SimulatorProcess {

    private static Logger logger = Logger.getLogger(SimulatorProcess.class);

    public static void processWithNoCache(String[] fileName) {
        StageDispatcher sd = new StageDispatcher("0", 4);
        double totalTime = 0;
        for(String application : fileName) {
            JobStageSubmitter jss = new JobStageSubmitter(application.split("_")[1], application);
            for(int i = 0; i < jss.jobList.size(); i++) {
                sd.dispatchStage(jss.submitAvailableJob());
                List<Stage> toSubmit = null;
                while((toSubmit = jss.submitAvailableStages()) != null) {
                    sd.dispatchStage(toSubmit);
                }
                double curTime = sd.runStages();
                logger.info(String.format("application [%s] job [%s] has run for [%f]s.)",
                        application, jss.jobList.get(i).jobId, curTime));
                totalTime += curTime;
            }
            logger.info(String.format("application [%s] has run for [%f]s.)",
                    application, totalTime));
        }
    }

}
