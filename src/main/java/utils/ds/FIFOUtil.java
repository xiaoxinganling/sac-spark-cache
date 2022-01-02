package utils.ds;

import entity.RDD;

public class FIFOUtil implements ReplaceUtil {
    @Override
    public boolean rddInCacheSpace(long rddId, boolean isHit) {
        return false;
    }

    @Override
    public boolean addRDD(RDD rdd) {
        return false;
    }

    @Override
    public void clear() {

    }
}
