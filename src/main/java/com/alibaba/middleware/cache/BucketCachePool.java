package com.alibaba.middleware.cache;

import com.alibaba.middleware.index.HashBucket;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
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
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private List<Integer> bucketIdList = new ArrayList<Integer>();
    private Random random = new Random();
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
        lock.writeLock().lock();
        if (!bucketCache.containsKey(bucketId)) {
            // TODO
            return null;
        }
        HashBucket bucket = bucketCache.get(bucketId);
        lock.writeLock().unlock();
        return bucketCache.get(bucketId);
    }

    public boolean addBucket(int bucketId, HashBucket bucket) {
        lock.writeLock().lock();
        if (bucketCounter.get() <= capacity) {
            bucketCache.put(bucketId, bucket);
            bucketCounter.getAndIncrement();
            return true;
        }
        lock.writeLock().unlock();
        return false;
    }

    private void removeBucket(int bucketId) {
        lock.writeLock().lock();
        if (bucketCache.containsKey(bucketId)) {
            bucketCache.get(bucketId).writeSelf();
            bucketCache.remove(bucketId);
            bucketCounter.getAndDecrement();
        }
        lock.writeLock().unlock();
    }

    /**
     * Randomly choose bucket to remove
     * */
    public void removeBucket() {
        bucketIdList.addAll(bucketCache.keySet());
        int removeIndex = random.nextInt(bucketIdList.size());
        removeBucket(bucketIdList.get(removeIndex));
        bucketIdList.clear();
    }
}
