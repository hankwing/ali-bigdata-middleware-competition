package com.alibaba.middleware.threads;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.conf.RaceConfig;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jelly
 *
 * {@link BucketCachePool} jvm monitor.
 * !important This isn't threadsafe, use single thread only. Be careful!
 *
 * Monitor JVM memory usage, if exceeds, write out {@link com.alibaba.middleware.index.HashBucket} in {@link BucketCachePool}
 */
public class JVMMonitorThread extends SchedulerThread {
    private boolean isReadyToStop = false;
    private String workerName = "JVMMonitor";
    private BucketCachePool bucketCachePool;
    private long maxMem = getMaxMem();
    private float memFactor = RaceConfig.memFactor;
    private int gcCounter = 0;
    private int gcCounterThreshold = RaceConfig.gcCounterThreshold;
    private long kb = 1024L;
    private long mb = 1024L * 1024L;
    private int removeBucketNum = RaceConfig.removeBucketNum;

    public JVMMonitorThread(BucketCachePool bucketCachePool) {
        this.bucketCachePool = bucketCachePool;
    }

    public JVMMonitorThread(String workerName, BucketCachePool bucketCachePool) {
        this.workerName = workerName;
        this.bucketCachePool = bucketCachePool;
    }

    @Override
    public String getWorkerName() {
        return workerName;
    }

    @Override
    public void setReadyToStop() {
        isReadyToStop = true;
    }

    @Override
    public boolean readyToStop() {
        return isReadyToStop;
    }

    @Override
    public void run() {
        try {
            if (!isReadyToStop) {
                if (getTotalMem() < (getMaxMem() * memFactor)) {
                    // TOTAL MEM IS TOO LITTLE, DO NOTHING
                } else if (getFreeMem() < (getMaxMem() * memFactor)) {
                    System.out.println(getFreeMem() / 2014 + "MB");
                    if (gcCounter < gcCounterThreshold) {
                        System.out.println("GC...");
                        System.gc();
                        gcCounter++;
                    } else {
                        System.out.println("Remove bucket...");
                        bucketCachePool.removeBuckets(removeBucketNum);
                        gcCounter = 0;
                    }
                }
            }
//            System.out.println("Stop");
        } catch (OutOfMemoryError e) {
            System.out.println("Out of memory, remove bucket...");
            bucketCachePool.removeBuckets(removeBucketNum);
            gcCounter = 0;
        }
    }

    // Currently free JVM memory from allocated
    public long getFreeMem() {
        return (Runtime.getRuntime().freeMemory() / 1024);
    }

    // Max JVM memory
    public long getMaxMem() {
        return (Runtime.getRuntime().maxMemory() / 1024);
    }

    // Currently allocated JVM memory
    public long getTotalMem() {
        return Runtime.getRuntime().totalMemory();
    }
}
