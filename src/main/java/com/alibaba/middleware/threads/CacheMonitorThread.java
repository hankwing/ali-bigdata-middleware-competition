package com.alibaba.middleware.threads;

import com.alibaba.middleware.cache.ConcurrentCache;
import com.alibaba.middleware.conf.RaceConfig;

/**
 * @author Jelly
 */
public class CacheMonitorThread extends SchedulerThread {
    private final long mb = 1024L * 1024L;
    private boolean isReadyToStop = false;
    private float cacheMemFactor = RaceConfig.cacheMemFactor;
    private ConcurrentCache cache;
    private long forceEvictNum = RaceConfig.forceEvictNum;

    public CacheMonitorThread(ConcurrentCache cache) {
        this.cache = cache;
    }

    @Override
    public void run() {
        try {
            if (!isReadyToStop) {
                //System.out.println("Now Used mem: " + (getTotalMem() - getFreeMem()) + "MB");
//                System.out.println("Now Max mem: " + Runtime.getRuntime().maxMemory()/mb + "MB");
                if ((getTotalMem() - getFreeMem()) > getMaxMem() * cacheMemFactor) {
                    System.out.println("Total mem: " + getTotalMem() + "MB");
                    System.out.println("Free mem: " + getFreeMem() + "MB");
                    cache.forceEvict(forceEvictNum);
                }
            }
        } catch (OutOfMemoryError e) {
            System.out.println("Out of memory, force evict");
            cache.forceEvict(forceEvictNum*10);
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

    @Override
    public String getWorkerName() {
        return "CacheMonitor";
    }

    @Override
    public void setReadyToStop() {
        isReadyToStop = true;
    }

    @Override
    public boolean readyToStop() {
        return isReadyToStop;
    }
}
