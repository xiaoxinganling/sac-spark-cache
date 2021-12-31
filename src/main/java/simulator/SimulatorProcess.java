package simulator;

import entity.Job;
import entity.RDD;
import entity.Stage;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class SimulatorProcess {

    private static Logger logger = Logger.getLogger(SimulatorProcess.class);

    public static void processWithNoCache(String[] applicationNames, String[] fileNames) {
        StageDispatcher sd = new StageDispatcher("0", 4);
        List<Double> applicationTimeToPrint = new ArrayList<>();
        for(int i = 0; i < applicationNames.length; i++) {
            String application = applicationNames[i];
            String applicationFileName = fileNames[i];
            JobStageSubmitter jss = new JobStageSubmitter(application, applicationFileName);
            double applicationTotalTime = 0;
            for(Job job : jss.jobList) {
                double jobTotalTime = 0;
                sd.dispatchStage(jss.submitAvailableJob());
                jobTotalTime += sd.runStages();
                List<Stage> toSubmit;
                while((toSubmit = jss.submitAvailableStages()) != null) {
                    sd.dispatchStage(toSubmit);
                    jobTotalTime += sd.runStages();
                }
                logger.info(String.format("SimulatorProcess: application [%s] job [%s] has run for [%f]s.)",
                        application, job.jobId, jobTotalTime));
                applicationTotalTime += jobTotalTime;
            }
            logger.debug(String.format("SimulatorProcess: application [%s] has run for [%f]s.)",
                    application, applicationTotalTime));
            applicationTimeToPrint.add(applicationTotalTime);
        }
        System.out.println(applicationTimeToPrint);
    }

    public static void processWithCache(String[] applicationNames, String[] fileNames) {
        StageDispatcher sd = new StageDispatcher("1", 4);
        CacheSpace cacheSpace = new CacheSpace(10, ReplacePolicy.FIFO);
        RDD tmpRDD = new RDD();
        tmpRDD.rddId = 38L;
        tmpRDD.memorySize = 1L;
        cacheSpace.addRDD(tmpRDD);
        List<Double> applicationTimeToPrint = new ArrayList<>();
        for(int i = 0; i < applicationNames.length; i++) {
            String application = applicationNames[i];
            String applicationFileName = fileNames[i];
            JobStageSubmitter jss = new JobStageSubmitter(application, applicationFileName);
            double applicationTotalTime = 0;
            for(Job job : jss.jobList) {
                double jobTotalTime = 0;
                sd.dispatchStage(jss.submitAvailableJob());
                jobTotalTime += sd.runStagesWithCacheSpace(cacheSpace);
                List<Stage> toSubmit;
                while((toSubmit = jss.submitAvailableStages()) != null) {
                    sd.dispatchStage(toSubmit);
                    jobTotalTime += sd.runStagesWithCacheSpace(cacheSpace);
                }
                logger.info(String.format("SimulatorProcess: application [%s] job [%s] has run for [%f]s.)",
                        application, job.jobId, jobTotalTime));
                applicationTotalTime += jobTotalTime;
            }
            logger.debug(String.format("SimulatorProcess: application [%s] has run for [%f]s.)",
                    application, applicationTotalTime));
            applicationTimeToPrint.add(applicationTotalTime);
        }
        System.out.println(applicationTimeToPrint);
    }

}