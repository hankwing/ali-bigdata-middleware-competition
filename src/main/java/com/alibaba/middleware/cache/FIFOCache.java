package com.alibaba.middleware.cache;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.index.HashBucket;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Jelly
 */
public class FIFOCache {

    private ConcurrentLinkedQueue<HashBucket> cache;
    private long maxCapacity = RaceConfig.threIndexFileCapacity;
    private DiskHashTable context = null;

    public FIFOCache(DiskHashTable context) {
    	this.context = context;
//        cache = new LinkedList<HashBucket>();
        cache = new ConcurrentLinkedQueue<HashBucket>();
    }

    public boolean addBucket(HashBucket bucket) {
        if (cache.add(bucket)) {
            return true;
        }
        return false;
    }

    public void removeBucket() {
        synchronized (cache) {
            HashBucket bucket = cache.poll();
            if (bucket != null) {
                bucket.writeSelfWhenBuilding();
                context.memRecordNum -= bucket.recordNum;
            }
        }
    }

    public boolean isReadyToRemove() { 
        return context.memRecordNum >= maxCapacity;
    }

    public synchronized int getSize() {
        return cache.size();
    }
}
