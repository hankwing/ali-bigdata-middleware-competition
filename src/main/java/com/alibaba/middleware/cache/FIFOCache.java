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
    private int capacity = RaceConfig.bucketCapcity;

    public FIFOCache() {
        cache = new LinkedList<HashBucket>();
    }

    public boolean addBucket(HashBucket bucket) {
        if (cache.add(bucket)) {
            return true;
        }
        return false;
    }

    public void removeBucket() {
        HashBucket bucket = cache.poll();
        if (bucket != null) {
            bucket.writeSelfAfterBuilding();
        }
    }

    public boolean isReadyToRemove() {
        return cache.size() >= capacity;
    }

    public synchronized int getSize() {
        return cache.size();
    }
}
