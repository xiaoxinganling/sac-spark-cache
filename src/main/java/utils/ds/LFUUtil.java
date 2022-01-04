package utils.ds;

import entity.RDD;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;


/**
 * from leetcode O(1) time algorithm, 先LFU再LRU
 */
public class LFUUtil extends ReplaceUtil {

    // key 到 val 的映射，我们后文称为 KV 表
    HashMap<Long, RDD> keyToVal;

    // key 到 freq 的映射，我们后文称为 KF 表
    HashMap<Long, Integer> keyToFreq;

    // freq 到 key 列表的映射，我们后文称为 FK 表
    HashMap<Integer, LinkedHashSet<Long>> freqToKeys;

    // 记录最小的频次
    int minFreq;

    public LFUUtil() {
        keyToVal = new HashMap<>();
        keyToFreq = new HashMap<>();
        freqToKeys = new HashMap<>();
        this.minFreq = Integer.MAX_VALUE; // 表示空间已空
    }

    @Override
    public long addRDD(RDD rdd) {
        long key = rdd.rddId;
        /* 若 key 已存在，修改对应的 val 即可 */ // TODO: 其实这段可以不要，不过还是保留语义吧 -> KEYPOINT: 这段还是保留好, 但是去除increaseFreq
        if (keyToVal.containsKey(key)) {
            // key 对应的 freq 加一
//            increaseFreq(key);
            return 0;
        }
        // 新rdd
        /* 插入 key 和 val，对应的 freq 为 1 */
        // 插入 KV 表
        keyToVal.put(key, rdd);
        // 插入 KF 表
        keyToFreq.put(key, 1);
        // 插入 FK 表
        freqToKeys.putIfAbsent(1, new LinkedHashSet<>());
        freqToKeys.get(1).add(key);
        // 插入新 key 后最小的 freq 肯定是 1
        this.minFreq = 1;
        return rdd.partitionNum;
    }

    @Override
    public RDD deleteRDD() {
        return removeMinFreqRDD();
    }

    @Override
    public void clear() {
        keyToVal.clear();
        keyToFreq.clear();
        freqToKeys.clear();
    }

    @Override
    public Set<Long> getCachedRDDIds() {
        return keyToVal.keySet();
    }

    @Override
    public RDD getRDD(long rddId) {
        if (!keyToVal.containsKey(rddId)) {
            return null;
        }
        // 增加 key 对应的 freq
        increaseFreq(rddId);
        return keyToVal.get(rddId);
    }

    private void increaseFreq(long key) {
        int freq = keyToFreq.get(key);
        /* 更新 KF 表 */
        keyToFreq.put(key, freq + 1);
        /* 更新 FK 表 */
        // 将 key 从 freq 对应的列表中删除
        freqToKeys.get(freq).remove(key);
        // 将 key 加入 freq + 1 对应的列表中
        freqToKeys.putIfAbsent(freq + 1, new LinkedHashSet<>());
        freqToKeys.get(freq + 1).add(key);
        // 如果 freq 对应的列表空了，移除这个 freq
        if (freqToKeys.get(freq).isEmpty()) {
            freqToKeys.remove(freq);
            // 如果这个 freq 恰好是 minFreq，更新 minFreq
            if (freq == this.minFreq) {
                this.minFreq++;
            }
        }
    }

    private RDD removeMinFreqRDD() {
        if(this.minFreq == Integer.MAX_VALUE) {
            return null;
        }
        // freq 最小的 key 列表
        LinkedHashSet<Long> keyList = freqToKeys.get(this.minFreq);
        // 其中最先被插入的那个 key 就是该被淘汰的 key
        long deletedKey = keyList.iterator().next();
        /* 更新 FK 表 */
        keyList.remove(deletedKey);
        if (keyList.isEmpty()) {
            freqToKeys.remove(this.minFreq);
            // 这里需要更新 minFreq 吗？ KEYPOINT:需要，因为我们一次删很多个key
            minFreq = Integer.MAX_VALUE;
            for(int freq : freqToKeys.keySet()) {
                minFreq = Math.min(freq, minFreq);
            }
        }
        /* 更新 KF 表 */
        keyToFreq.remove(deletedKey);
        RDD toDelete = keyToVal.get(deletedKey);
        /* 更新 KV 表 */
        keyToVal.remove(deletedKey);
        return toDelete;
    }

}