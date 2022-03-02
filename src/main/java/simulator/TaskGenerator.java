package simulator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import entity.Job;
import entity.Task;
import entity.event.StageCompletedEvent;
import sketch.StaticSketch;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaskGenerator {

    /**
     * 根据application文件名读取所有task（task list无序）
     */
    public static Map<Long, List<Task>> generateTaskOfApplication(String fileName) throws IOException {
        String fileNameWithTask = fileName + "_task";
        // check whether file_with_task is existing
        // for accelerating
        if (new File(fileNameWithTask).exists()) {
            Map<Long, List<Task>> stageIdToTaskList = new HashMap<>();
            BufferedReader br = new BufferedReader(new FileReader(fileNameWithTask));
            String line;
            while((line = br.readLine()) != null) {
                Task curTask = JSON.toJavaObject(JSONObject.parseObject(line), Task.class);
                List<Task> curTaskList = stageIdToTaskList.getOrDefault(curTask.stageId, new ArrayList<>());
                curTaskList.add(curTask);
                stageIdToTaskList.put(curTask.stageId, curTaskList);
            }
            br.close();
        }
        // initial read
        Map<Long, List<Task>> stageIdToTaskList = new HashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String line;
        while((line = br.readLine()) != null) {
            JSONObject jsonObject = JSONObject.parseObject(line);
            if (jsonObject.get("Event").equals(StaticSketch.taskEndEventFlag)) {
                Task curTask = JSON.toJavaObject(jsonObject, Task.class);
                List<Task> curTaskList = stageIdToTaskList.getOrDefault(curTask.stageId, new ArrayList<>());
                curTaskList.add(curTask);
                stageIdToTaskList.put(curTask.stageId, curTaskList);
            }
        }
        br.close();
        // initial write
        BufferedWriter bw = new BufferedWriter(new FileWriter(fileNameWithTask));
        for (List<Task> taskList : stageIdToTaskList.values()) {
            for (Task task : taskList) {
                bw.write(JSON.toJSON(task).toString() + "\n");
            }
        }
        bw.close();
        return stageIdToTaskList;
    }

    public static void updateTaskTimeWithMaxTime(Map<Long, List<Task>> stageIdToTaskList) {
        for (long key : stageIdToTaskList.keySet()) {
            List<Task> tasks = stageIdToTaskList.get(key);
            long minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
            for (Task task : tasks) {
                minTime = Math.min(minTime, task.taskInfo.startTime);
                maxTime = Math.max(maxTime, task.taskInfo.finishTime);
            }
            for (Task task : tasks) {
                task.setDuration(maxTime - minTime);
            }
        }
    }

    public static Map<Long, Task> generateTaskMap(Map<Long, List<Task>> stageIdToTasks) {
        Map<Long, Task> taskMap = new HashMap<>();
        for (List<Task> tasks : stageIdToTasks.values()) {
            for (Task t : tasks) {
                assert !taskMap.containsKey(t.getTaskId());
                taskMap.put(t.getTaskId(), t);
            }
        }
        return taskMap;
    }

}
