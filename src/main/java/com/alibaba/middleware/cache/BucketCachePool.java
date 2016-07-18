package com.alibaba.middleware.cache;

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

    private HashMap<Integer, HashBucket> bucketCache;
    private int capacity;
    // Number of hash buckets
    private AtomicInteger bucketCounter = new AtomicInteger(0);
    private Lock lock = new ReentrantLock();
    private Queue<Integer> bucketFIFO = new LinkedList<Integer>();
    private static BucketCachePool instance;

    private BucketCachePool(int capacity) {
        this.capacity = capacity;
        bucketCache = new HashMap<Integer, HashBucket>(capacity);
    }

    public static BucketCachePool getInstance(int capacity) {
        if (instance == null)
            instance = new BucketCachePool(capacity);
        return instance;
    }

    public HashBucket getBucket(int bucketId) {
        lock.lock();
        if (!bucketCache.containsKey(bucketId)) {
            // TODO
            return null;
        }
        HashBucket bucket = bucketCache.get(bucketId);
        lock.unlock();
        return bucketCache.get(bucketId);
    }

    public boolean addBucket(int bucketId, HashBucket bucket) {
        if (bucketCounter.get() <= capacity) {
            lock.lock();
            bucketCache.put(bucketId, bucket);
            bucketCounter.getAndIncrement();
            bucketFIFO.add(bucketId);
            lock.unlock();
            return true;
        }
        return false;
    }

    private void removeBucket(int bucketId) {
        lock.lock();
        if (bucketCache.containsKey(bucketId)) {
            bucketCache.get(bucketId).writeSelf();
            bucketCache.remove(bucketId);
            bucketCounter.getAndDecrement();
        }
        lock.unlock();
    }

    /**
     * Remove N buckets based on FIFO
     * */
    public void removeBuckets(int num) {
        lock.lock();
        for (int i = 0; i < num; i++) {
            removeBucket(bucketFIFO.poll());
        }
        lock.unlock();
    }
}
