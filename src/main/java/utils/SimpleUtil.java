package utils;

import entity.Stage;
import entity.event.JobStartEvent;
import java.util.HashSet;
import java.util.List;

public class SimpleUtil {

    /**
     *
     * @param jobs
     * @return the number of stage in the provided job list
     */
    public static int stageNumOfJobs(List<JobStartEvent> jobs) {
        HashSet<Long> stageSet = new HashSet<>();
        int res = 0;
        for(JobStartEvent jse : jobs) {
            for(Stage stage : jse.stages) {
                assert(!stageSet.contains(stage.stageId));
                stageSet.add(stage.stageId);
            }
            res += jse.stages.size();
        }
        return res;
    }

}
