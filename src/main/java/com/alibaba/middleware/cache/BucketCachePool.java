package com.alibaba.middleware.cache;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.index.HashBucket;

/**
 * @author Jelly
 *
 * Used for HashBucket.
 * 固定数目的HashBucket,每当bucket达到上限,写入磁盘
 */
public class BucketCachePool {
    private int bucketCapacity = RaceConfig.bucketCapcity;
    // Number of hash buckets
    private AtomicInteger bucketCounter = new AtomicInteger(0);
    //private Lock lock = new ReentrantLock();
    private LinkedBlockingQueue<HashBucket> bucketCache;
    private static BucketCachePool instance;

    private BucketCachePool() {
        bucketCache = new LinkedBlockingQueue<HashBucket>();
    }

    public static BucketCachePool getInstance() {
        if (instance == null)
            instance = new BucketCachePool();
        return instance;
    }

    /**
     * 设桶的上限
     * @param bucket
     * @return
     */
    public boolean addBucket(HashBucket bucket) {
        if (bucketCounter.get() <= bucketCapacity) {
            bucketCache.offer(bucket);
            bucketCounter.getAndIncrement();
            return true;
        } else {
        	System.out.println("bucket too much in cache");
            removeBuckets(RaceConfig.bucketRemoveNum);
            return false;
        }
    }
    
    /**
     * 将管理的桶清空
     */
    public synchronized void removeAllBucket() {
    	bucketCache = new LinkedBlockingQueue<HashBucket>();
    }

    private void removeBucket(HashBucket bucket) {
        if( bucket != null ) {
        	bucket.writeSelfAfterBuilding();
//            bucket = null;
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
            removeBucket(bucketCache.poll());
        }
    }
}
