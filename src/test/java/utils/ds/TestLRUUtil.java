package utils.ds;

import entity.RDD;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TestLRUUtil {

    @Test
    void testOrder() {
        LRUUtil lruUtil = new LRUUtil();
        // add [0,1,2,3,4] and print
        for(int i = 0; i < 5; i++) {
            RDD tmp = new RDD();
            tmp.rddId = (long) i;
            tmp.partitionNum = (long) 1;
            lruUtil.addRDD(tmp);
        }
        System.out.println(lruUtil.getCachedRDDIds());
        // delete [0]
        System.out.println(lruUtil.deleteRDD());
        // cur order: [2,3,4,1] -> newer
        lruUtil.getRDD(1L);
        // delete [2]
        System.out.println(lruUtil.deleteRDD());
        // remaining: [3,4,1]
        System.out.println(lruUtil.getCachedRDDIds());
        // delete [3]
        System.out.println(lruUtil.deleteRDD());
        // cur order: [1,4] -> newer
        lruUtil.getRDD(4L);
        // delete [1]
        System.out.println(lruUtil.deleteRDD());
        // delete [4]
        System.out.println(lruUtil.deleteRDD());
        // delete [null]
        System.out.println(lruUtil.deleteRDD());
    }

}