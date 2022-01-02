package utils.ds;


import entity.RDD;

/**
 * 实现替换算法的工具接口，定义了需实现的方法
 */
public interface ReplaceUtil {

    boolean rddInCacheSpace(long rddId, boolean isHit);

    boolean addRDD(RDD rdd);

    void clear();

}
