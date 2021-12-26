import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import entity.sac.CacheCandidate;
import entity.RDD;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;
import entity.sac.CacheCandidateRule;
import utils.CacheSketcher;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class StaticSketch {
    public final static String stageCompletedEventFlag = "SparkListenerStageCompleted";
    public final static String jobStartEventFlag = "SparkListenerJobStart";
    public final static String[] applicationName = {"spark_connected_component", "spark_page_rank", "spark_svd++", "spark_pregel_operation",
    "spark_shorted_path", "spark_svm", "spark_strongly_connected_component",
    "spark_linear_regression", "spark_logistic_regression", "spark_matrix_factorization",
    "spark_triangle_count", "spark_rdd_relation", "spark_kmeans", "spark_label_propagation",
    "spark_decision_tree", "spark_pca", "spark_tera_sort"};//"spark_hive", "spark_rdd_relation", spark_sql, spark_page_view
    public final static String[] applicationPath = {"application_1634644457236_0036",
    "application_1634644457236_0034",
    "application_1634644457236_0037",
    "application_1634644457236_0035",
    "application_1634644457236_0038",
    "application_1634644457236_0039",
    "application_1634644457236_0040",
    "application_1639021055367_0004",
    "application_1639021055367_0009",
    "application_1639021055367_0014",
    "application_1639021055367_0011",
    "application_1639021055367_0013",
    "application_1639021055367_0017",
    "application_1639021055367_0015",
    "application_1639021055367_0019",
    "application_1639021055367_0027",
    "application_1639021055367_0026"};
    public static void main(String[] args) throws Exception {
        // 1. 读取文件
        String fileName = "E:\\Google Chrome Download\\application_1634644457236_0031_1";
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String line;
        while((line = br.readLine()) != null) {
            JSONObject jsonObject = JSONObject.parseObject(line);
            if(jsonObject.get("Event").equals(stageCompletedEventFlag)) {
                StageCompletedEvent sce = JSON.toJavaObject(jsonObject, StageCompletedEvent.class);
                System.out.println(sce.stage);
                //System.out.println(sce.stageList.get(0).stageName);
            }
        }
    }

    public static void generateJobAndStageList(List<JobStartEvent> jobList, List<StageCompletedEvent> stageList, String fileName) throws IOException {
        assert jobList.size() == 0;
        assert stageList.size() == 0;
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String line;
        while((line = br.readLine()) != null) {
            JSONObject jsonObject = JSONObject.parseObject(line);
            if(jsonObject.get("Event").equals(stageCompletedEventFlag)) {
                StageCompletedEvent stage = JSON.toJavaObject(jsonObject, StageCompletedEvent.class);
                stageList.add(stage);
            }else if(jsonObject.get("Event").equals(jobStartEventFlag)) {
                jobList.add(JSON.toJavaObject(jsonObject, JobStartEvent.class));
            }
        }
        br.close();
    }

    // generate raw cache candidates and sort them by asc order
    public static List<CacheCandidate> generateSortedRawCacheCandidatesByHistory(String fileName, boolean isDirect) throws IOException {
        // 1. get job/stage list
        List<JobStartEvent> jobList = new ArrayList<>();
        List<StageCompletedEvent> stageList = new ArrayList<>();
        generateJobAndStageList(jobList, stageList, fileName);
        // 2. get ref/direct_ref/hop_compute_time/partition
        Map<Long, Integer> ref = CacheSketcher.generateRefForJobs(jobList);
        Map<Long, Integer> directRef = CacheSketcher.generateRDDDirectRefForJobs(jobList, null);
        Map<Long, Integer> hopComputeTime = CacheSketcher.generateHopComputeTimeForJobs(jobList);
        Map<Long, Long> partitions = CacheSketcher.generatePartitionForJobs(jobList);
        // 3. filter rdd
        Map<Long, RDD> rddMap = new HashMap<>();
        for(StageCompletedEvent sce : stageList) {
            // FIXME: 去重, 单个Stage考虑能将indegree考虑完全吗？
            for(RDD rdd : sce.stage.rdds) {
                if(rddMap.containsKey(rdd.rddId)) {
                    assert rddMap.get(rdd.rddId).rddParentIDs.equals(rdd.rddParentIDs);
                }else{
                    rddMap.put(rdd.rddId, rdd);
                }
            }
            //FIXME: 他俩的hashcode一样，所以直接判断为相等了，不太符合邏輯
        }
        List<CacheCandidate> rawCC = new ArrayList<>();
        for(Map.Entry<Long, RDD> entry : rddMap.entrySet()) {
            long rddId = entry.getKey();
            CacheCandidate cc = new CacheCandidate(rddId, ref.get(rddId), directRef.get(rddId),
                    hopComputeTime.get(rddId), partitions.get(rddId));
            cc.setParent(entry.getValue().rddParentIDs);
            rawCC.add(cc);
        }
        // 4. filter cache candidates
        rawCC.sort(isDirect ? ((Comparator<CacheCandidate>) (o1, o2) -> {
            if (o1.getDirectCe() > o2.getDirectCe()) {
                return 1;
            } else if (o1.getDirectCe() == o2.getDirectCe()) {
                return 0;
            }
            return -1;
        }) : ((Comparator<CacheCandidate>) (o1, o2) -> {
            if (o1.getCe() > o2.getCe()) {
                return 1;
            } else if (o1.getCe() == o2.getCe()) {
                return 0;
            }
            return -1;
        }));
        return rawCC;
    }

    // generate target cache candidates according to rules and raw-sorted cache candidates
    public static List<CacheCandidate> generateTargetCacheCandidates(List<CacheCandidate> rawCC, CacheCandidateRule rule) {
        boolean isGreedy = rule.isGreedy();
        if(isGreedy) {
            return rawCC;
        }
        List<CacheCandidate> cacheCandidates = new ArrayList<>();
        boolean isDirect = rule.isDirect();
        float threshold = rule.getThreshold();
        float max = Float.MIN_VALUE;
        float min = Float.MAX_VALUE;
        for(CacheCandidate cc: rawCC) {
            float ce = isDirect ? cc.getDirectCe() : cc.getCe();
            max = Math.max(max , ce);
            min = Math.min(min, ce);
        }
        // KEYPOINT: i from `rawCC.size() - 1` to 0
        for(int i = rawCC.size() - 1; i >= 0; i--) {
            CacheCandidate cc = rawCC.get(i);
            float ce = isDirect ? cc.getDirectCe() : cc.getCe();
            if(ce < threshold * (max - min)) {
                break;
            }
            cacheCandidates.add(cc);
        }
        return cacheCandidates;
    }

}
