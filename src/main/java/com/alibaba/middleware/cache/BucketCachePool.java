package com.alibaba.middleware.cache;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.index.HashBucket;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Jelly
 *
 * Used for HashBucket.
 * 固定数目的HashBucket,每当bucket达到上限,写入磁盘
 */
public class BucketCachePool {

    private int capacity;
    // Number of hash buckets
    private AtomicInteger bucketCounter = new AtomicInteger(0);
    private Lock lock = new ReentrantLock();
    private Queue<HashBucket> bucketCache;
    private static BucketCachePool instance;

    private BucketCachePool() {
        this.capacity = RaceConfig.bucketCachePoolCapacity;
        bucketCache = new LinkedList<HashBucket>();
    }

    public static BucketCachePool getInstance() {
        if (instance == null)
            instance = new BucketCachePool();
        return instance;
    }

    public boolean addBucket(HashBucket bucket) {
        if (bucketCounter.get() <= capacity) {
            lock.lock();
            bucketCache.add(bucket);
            bucketCounter.getAndIncrement();
            lock.unlock();
            return true;
        }
        return false;
    }
    
    /**
     * 将管理的桶清空
     */
    public synchronized void removeAllBucket() {
    	bucketCache = new LinkedList<HashBucket>();
    }

    private void removeBucket(HashBucket bucket) {
        lock.lock();
        bucket.writeSelf();
        bucket = null;
        lock.unlock();
        bucketCounter.getAndDecrement();
    }

    /**
     * Remove N buckets based on FIFO
     * */
    public void removeBuckets(int num) {
        lock.lock();
        if (num > bucketCounter.get()) {
            num = bucketCounter.get();
        }
        for (int i = 0; i < num; i++) {
            removeBucket(bucketCache.poll());
        }
        lock.unlock();
    }
}
