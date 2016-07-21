package com.alibaba.middleware.cache;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.index.HashBucket;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
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

    // Number of hash buckets
    private AtomicInteger bucketCounter = new AtomicInteger(0);
    //private Lock lock = new ReentrantLock();
    private LinkedBlockingQueue<HashBucket> bucketCache;
    private static BucketCachePool instance;

    private BucketCachePool() {
        //this.capacity = RaceConfig.bucketCachePoolCapacity;
        bucketCache = new LinkedBlockingQueue<HashBucket>();
    }

    public static BucketCachePool getInstance() {
        if (instance == null)
            instance = new BucketCachePool();
        return instance;
    }

    /**
     * 先不设桶的上限了 反正是根据内存使用来释放桶
     * @param bucket
     * @return
     */
    public boolean addBucket(HashBucket bucket) {
        //if (bucketCounter.get() <= capacity) {
            bucketCache.offer(bucket);
            bucketCounter.getAndIncrement();
            return true;
       // }
    }
    
    /**
     * 将管理的桶清空
     */
    public synchronized void removeAllBucket() {
    	bucketCache = new LinkedBlockingQueue<HashBucket>();
    }

    private void removeBucket(HashBucket bucket) {
        if( bucket != null ) {
        	bucket.writeSelf();
            bucket = null;
            bucketCounter.getAndDecrement();
        }
        
    }

    /**
     * Remove N buckets based on FIFO
     * */
    public void removeBuckets(int num) {
        if (num > bucketCounter.get()) {
            num = bucketCounter.get();
        }
        for (int i = 0; i < num; i++) {
        	System.out.println("remove bucket!!");
            removeBucket(bucketCache.poll());
        }
    }
}
