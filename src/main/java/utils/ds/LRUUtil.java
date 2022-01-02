package utils.ds;


import entity.RDD;

import java.util.LinkedHashMap;

public class LRUUtil {

    private LinkedHashMap<Long, RDD> memoryMap;

    public LRUUtil() {
        memoryMap = new LinkedHashMap<Long, RDD>(16, 0.75f, true);
    }
}
