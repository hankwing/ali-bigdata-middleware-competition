package com.alibaba.middleware.cache;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.index.HashBucket;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Jelly
 */
public class FIFOCache {
    Queue<HashBucket> cache;
    private long capacity = 0;
    private long maxCapacity = RaceConfig.bigIndexFileCapacity;

    public FIFOCache() {
        cache = new LinkedList<HashBucket>();
    }

    public boolean addBucket(HashBucket bucket) {
        if (cache.add(bucket)) {
        	capacity += bucket.recordNum;
            return true;
        }
        return false;
    }

    public void removeBucket() {
        HashBucket bucket = cache.poll();
        if (bucket != null) {
            bucket.writeSelfAfterBuilding();
            capacity -= bucket.recordNum;
        }
    }

    public boolean isReadyToRemove() {
        return capacity >= maxCapacity;
    }

    public synchronized int getSize() {
        return cache.size();
    }
}
