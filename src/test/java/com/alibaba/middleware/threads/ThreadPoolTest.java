package com.alibaba.middleware.threads;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.index.HashBucket;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jelly
 */
public class ThreadPoolTest {
    private static ThreadPool threadPool = ThreadPool.getInstance();

    public static void main(String[] args) {
//        exampleThreadTest();
        jvmMonitorThreadTest();
    }

    public static void exampleThreadTest() {
        for (int i = 0; i < 5; i++) {
            ExampleThread exampleThread = new ExampleThread("worker" + i);
            threadPool.addWorker(exampleThread);
        }
        threadPool.startWorkers();
        threadPool.stopWorkers();
        threadPool.shutdown();
    }

    public static void jvmMonitorThreadTest() {
        BucketCachePool bucketCachePool = new BucketCachePool(RaceConfig.bucketCachePoolCapacity);
        DiskHashTable<Integer, String> context = new DiskHashTable<Integer, String>();
        JVMMonitorThread jvmMonitorThread = new JVMMonitorThread(bucketCachePool);
        threadPool.addMonitor(jvmMonitorThread);
        threadPool.startMonitors();
        for (int i = 0; i < 1000; i++) {
            HashBucket<Integer, String> bucket = new HashBucket<Integer, String>(context, i, String.class);
            for (int j = 0; i < 1000; j++) {
                bucket.putAddress("", i, new Long(i+j));
            }
            bucketCachePool.addBucket(i, bucket);
        }
        threadPool.stopMonitors();
        threadPool.shutdown();
    }
}

class ExampleThread extends WorkerThread {
    private String workerName = "ExampleThread";
    private AtomicBoolean isReadyToStop = new AtomicBoolean(true);

    public ExampleThread() {

    }

    public ExampleThread(String workerName) {
        this.workerName = workerName;
    }

    @Override
    public String getWorkerName() {
        return workerName;
    }

    @Override
    public void setReadyToStop() {
        isReadyToStop.set(true);
    }

    @Override
    public boolean readyToStop() {
        return isReadyToStop.get();
    }

    @Override
    public void run() {
        while (!readyToStop()) {
            // DO NOTHING
        }
        System.out.println(getWorkerName()+" stops");
    }
}
