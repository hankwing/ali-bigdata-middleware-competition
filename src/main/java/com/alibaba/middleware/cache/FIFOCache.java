package com.alibaba.middleware.cache;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.index.HashBucket;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Jelly
 */
public class FIFOCache {

	private ReentrantReadWriteLock readWriteLock = null;
    Queue<HashBucket> cache;
    private long maxCapacity = RaceConfig.threIndexFileCapacity;
    private DiskHashTable context = null;

    public FIFOCache(DiskHashTable context) {
    	this.context = context;
        cache = new LinkedList<HashBucket>();
        readWriteLock = new ReentrantReadWriteLock(true);
    }

    public boolean addBucket(HashBucket bucket) {
    	//readWriteLock.writeLock().lock();
        if (cache.add(bucket)) {
            return true;
        }
        //readWriteLock.writeLock().unlock();
        return false;
    }

    public void removeBucket() {
    	//readWriteLock.writeLock().lock();
        HashBucket bucket = cache.poll();
        //readWriteLock.writeLock().unlock();
        if (bucket != null) {
            bucket.writeSelfWhenBuilding();
            context.memRecordNum -= bucket.recordNum;
        }
        
    }

    public boolean isReadyToRemove() { 
        return context.memRecordNum >= maxCapacity;

    }

    public synchronized int getSize() {
        return cache.size();
    }
}
