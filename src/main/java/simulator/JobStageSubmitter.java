package simulator;

import entity.Job;
import entity.Stage;
import lombok.Data;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

@Data
public class JobStageSubmitter {

    private Logger logger = Logger.getLogger(this.getClass());

    private String id;

    // 持有的job list
    public List<Job> jobList;

    // 当前提交的job下标
    private int curSubmittedJob;

    // 当前提交的job中已提交的stage id
    private Set<Long> submittedStageInCurJob;

    // 当前提交的job中未提交的stage map
    private Map<Long, Stage> unSubmittedStageInCurJobMap;

    public JobStageSubmitter(String id, String application) {
        logger.info(String.format("JobSubmitter [%s] is created based on application [%s].", id, application));
        try {
            jobList = JobGenerator.generateJobsWithFilteredStagesOfApplication(application);
            curSubmittedJob = -1;
            submittedStageInCurJob = null;
            unSubmittedStageInCurJobMap = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 提交一次job的同时提交一次可用stage，后续伴随多次stage提交
     * @return
     */
    public List<Stage> submitAvailableJob() {
        if(curSubmittedJob >= jobList.size() - 1) {
            return null;
        }
        curSubmittedJob++;
        unSubmittedStageInCurJobMap = new HashMap<>();
        logger.info(String.format("JobSubmitter [%s] is submitting Job [%d].", id, jobList.get(curSubmittedJob).jobId));
        for(Stage stage : jobList.get(curSubmittedJob).stages) {
            unSubmittedStageInCurJobMap.put(stage.stageId, stage);
        }
        submittedStageInCurJob = new HashSet<>();
        return submitAvailableStages();
    }

    /**
     * 提交一次可用stage，返回null表示当前job已无stage提交
     * @return
     */
    public List<Stage> submitAvailableStages() {
        if(curSubmittedJob == -1 || submittedStageInCurJob.size() >= jobList.get(curSubmittedJob).stages.size()) {
            return null; // need to submit job first
        }
        List<Stage> stageToSubmit = new ArrayList<>();
        for(Stage stage : unSubmittedStageInCurJobMap.values()) {
            boolean isAvailable = true;
            for(long parentId : stage.parentIDs) {
                if(unSubmittedStageInCurJobMap.containsKey(parentId)) {
                    isAvailable = false;
                    break;
                }
            }
            if(isAvailable) {
                stageToSubmit.add(stage);
            }
        }
        StringBuilder toSubmitStr = new StringBuilder();
        for(Stage toSubmit : stageToSubmit) {
            submittedStageInCurJob.add(toSubmit.stageId);
            unSubmittedStageInCurJobMap.remove(toSubmit.stageId);
            toSubmitStr.append(toSubmit.stageId).append(",");
        }
        toSubmitStr.deleteCharAt(toSubmitStr.length() - 1);
        logger.info(String.format("JobSubmitter [%s] is submitting Stages [%s].", id, toSubmitStr.toString()));
        return stageToSubmit;
    }

}
