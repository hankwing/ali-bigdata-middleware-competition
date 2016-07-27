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
    private float memFactor = RaceConfig.memFactor;
    private int gcCounter = 0;
    private int gcCounterThreshold = RaceConfig.gcCounterThreshold;
    private long kb = 1024L;
    private long mb = 1024L * 1024L;
    private int removeBucketNum = RaceConfig.removeBucketNum;

    public JVMMonitorThread(String workerName) {
        this.workerName = workerName;
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
               //System.out.println("Now Used mem: " + (getTotalMem() - getFreeMem()) + "MB");
                //System.out.println("Now Max mem: " + Runtime.getRuntime().maxMemory()/mb + "MB");
                if ((getTotalMem() - getFreeMem()) > getMaxMem() * memFactor) {
                    System.out.println("Total mem: " + getTotalMem() + "MB");
                    System.out.println("Free mem: " + getFreeMem() + "MB");
//                    if (gcCounter < gcCounterThreshold) {
//                        gcCounter++;
//                    } else {
//                        System.out.println("Remove bucket...");
////                        bucketCachePool.removeBuckets(removeBucketNum);
//                        gcCounter = 0;
//                    }
                    System.gc();
                }
            }
//            System.out.println("Stop");
        } catch (OutOfMemoryError e) {
            System.out.println("Out of memory, gc...");
            System.gc();
//            bucketCachePool.removeBuckets(removeBucketNum);
        }
    }

    // Currently free JVM memory from allocated
    public long getFreeMem() {
        return (Runtime.getRuntime().freeMemory()/mb);
    }

    // Max JVM memory
    public long getMaxMem() {
        return (Runtime.getRuntime().maxMemory()/mb);
    }

    // Currently allocated JVM memory
    public long getTotalMem() {
        return (Runtime.getRuntime().totalMemory()/mb);
    }
}
