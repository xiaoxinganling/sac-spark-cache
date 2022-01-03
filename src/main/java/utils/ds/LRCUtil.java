package utils.ds;

import entity.RDD;
import java.util.Set;

/**
 * my own implementation of `LRC`: least reference count
 */
public class LRCUtil extends ReplaceUtil {

    // RCMap -> init at LRCUtil(), update at addRDD()
    // DS for saving
    // idSet

    @Override
    public long addRDD(RDD rdd) {
        return 0;
    }

    @Override
    public RDD deleteRDD() {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public Set<Long> getCachedRDDIds() {
        return null;
    }

    @Override
    public RDD getRDD(long rddId) {
        return null;
    }
}
