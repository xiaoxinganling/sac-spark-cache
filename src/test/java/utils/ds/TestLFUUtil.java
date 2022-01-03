package utils.ds;

import entity.RDD;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TestLFUUtil {

    @Test
    void testOrder() {
        LFUUtil lfuUtil = new LFUUtil();
        // add [0,1,2,3,4] and print
        for(int i = 0; i < 5; i++) {
            RDD tmp = new RDD();
            tmp.rddId = (long) i;
            tmp.partitionNum = (long) 1;
            lfuUtil.addRDD(tmp);
        }
        // delete [0]
        System.out.println(lfuUtil.keyToFreq + " " + lfuUtil.deleteRDD().rddId);
        // delete [3]
        lfuUtil.getRDD(1L);
        lfuUtil.getRDD(1L);
        lfuUtil.getRDD(2L);
        System.out.println("+1\t+1\t+2");
        System.out.println(lfuUtil.keyToFreq + " " + lfuUtil.deleteRDD().rddId);
        // delete [2]
        lfuUtil.getRDD(4L);
        lfuUtil.getRDD(4L);
        System.out.println("+4\t+4");
        System.out.println(lfuUtil.keyToFreq + " " + lfuUtil.deleteRDD().rddId);
        // delete [4]
        RDD tmp = new RDD();
        tmp.rddId = 1L;
        tmp.partitionNum = 1L;
        lfuUtil.addRDD(tmp);
        System.out.println("+1");
        System.out.println(lfuUtil.keyToFreq + " " + lfuUtil.deleteRDD().rddId);
        System.out.println(lfuUtil.keyToFreq + " " + lfuUtil.deleteRDD().rddId);
    }

}