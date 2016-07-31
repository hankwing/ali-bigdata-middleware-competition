package com.alibaba.middleware.cache;

import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.index.HashBucket;
import com.alibaba.middleware.threads.BucketMonitorThread;
import com.alibaba.middleware.threads.FIFOCacheMonitorThread;

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
    static FIFOCache cache1 = new FIFOCache(null);
    static FIFOCache cache2 = new FIFOCache(null);

    public static void main(String[] args) {
        FIFOCacheMonitorThread thread = FIFOCacheMonitorThread.getInstance();
        thread.registerFIFIOCache(cache1);
        thread.registerFIFIOCache(cache2);

        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(thread);

        DiskHashTable diskHashTable = new DiskHashTable();

        for (int i = 0; i < 10; i++) {
            cache1.addBucket(new HashBucket(diskHashTable, i));
            System.out.println("Size: " + cache1.getSize());
         }

        System.out.println("Size: " + cache1.getSize());
        int size = cache1.getSize();

        for (int i = 0; i < size; i++) {
//            System.out.println(i);
            System.out.println("cache: ");
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(cache1.getSize());
//        size = cache1.getSize();
//        for (int i = 0; i < size; i++) {
//            System.out.println("After cache: ");
//        }
    }
}
