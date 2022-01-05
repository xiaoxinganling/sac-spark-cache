package utils.ds;


import entity.RDD;

import java.util.Map;
import java.util.Set;

/**
 * 实现替换算法的工具接口，定义了需实现的方法
 * rddInCacheSpace: 判断rdd是否在CacheSpace中
 * addRDD：添加RDD到CacheSpace
 * deleteRDD：从CacheSpace中删除RDD
 * clear：清除CacheSpace
 * getCachedRDDIds：返回RDDId的set集合
 * init: 初始化CacheSpace的数据结构
 */
public abstract class ReplaceUtil {

    protected Set<Long> cachedRDDIds;

    public abstract long addRDD(RDD rdd);

    public abstract RDD deleteRDD();

    public abstract void clear();

    public abstract Set<Long> getCachedRDDIds();

    public abstract RDD getRDD(long rddId);

    public abstract Map getPriority();

}
