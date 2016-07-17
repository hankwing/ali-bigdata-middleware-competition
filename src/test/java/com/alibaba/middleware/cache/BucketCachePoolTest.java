package com.alibaba.middleware.cache;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.index.DiskHashTable;

/**
 * @author Jelly
 *
 * unfinished
 */
public class BucketCachePoolTest {
    private static BucketCachePool bucketCachePool = new BucketCachePool(RaceConfig.bucketCachePoolCapacity);
    private static DiskHashTable<Integer, String> diskHashTable = new DiskHashTable<Integer, String>("/Users/Jelly/Developer", "", String.class);

    public static void main(String[] args) {
        for (int i = 0; i < 100000; i++) {
            diskHashTable.put(i, Long.valueOf(i*2));
        }
        //TODO
    }
}
