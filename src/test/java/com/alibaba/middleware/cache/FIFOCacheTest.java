package com.alibaba.middleware.cache;

import com.alibaba.middleware.index.HashBucket;
import com.alibaba.middleware.threads.BucketMonitorThread;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public class FIFOCacheTest {
    static FIFOCache cache1 = new FIFOCache();
    static FIFOCache cache2 = new FIFOCache();

    public static void main(String[] args) {
        BucketMonitorThread thread = new BucketMonitorThread();
        thread.registerFIFIOCache(cache1);

        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(thread);

        for (int i = 0; i < 10; i++) {
//            cache1.addBucket(i);
            System.out.println("Size: " + cache1.getSize());
         }

        System.out.println("Size: " + cache1.getSize());
        int size = cache1.getSize();

        for (int i = 0; i < size; i++) {
//            System.out.println(i);
            System.out.println("cache: " + cache1.cache.peek());
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        size = cache1.getSize();
        for (int i = 0; i < size; i++) {
//            System.out.println("After cache: " + cache1.removeBucket());
        }
    }
}
