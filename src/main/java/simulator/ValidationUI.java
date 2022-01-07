package simulator;

import entity.RDD;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * 检验替换算法是否work，将结果输出到csv
 */
public class ValidationUI {

    private static final String SVM_VALID = "a_spark_svm_validation_N.tsv";

    private static boolean isInitial = true;

    // only id
    public static void writeLine(long jobId, long stageId, RDD rdd, Set<Long> beforeCS, Set<Long> afterCS,
                                 Map beforePriority, ReplacePolicy policy, Map afterPriority) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(SVM_VALID, true));
        if (isInitial) {
            bw.write(String.format("%s\n", policy));
            bw.write("job\tstage\thot_rdd\tbefore_cache_space\tafter_cache_space\tbefore_prior\tafter_prior\tchanged\n");
            isInitial = false;
        }
        bw.write(String.format("%d\t%d\t%d:%d\t%s\t%s\t%s\t%s\t%s\n",
                jobId, stageId, rdd.rddId, rdd.partitionNum, beforeCS, afterCS, beforePriority, afterPriority,
                !beforeCS.toString().equals(afterCS.toString())));
        bw.close();
    }

}
