package com.alibaba.middleware.threads;

import com.alibaba.middleware.cache.FIFOCache;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jelly
 */
public class FIFOCacheMonitorThread extends WorkerThread {
    private List<FIFOCache> cacheList;
    private static FIFOCacheMonitorThread instance = null;

    private FIFOCacheMonitorThread() {
        cacheList = new ArrayList<FIFOCache>();
    }

    public static FIFOCacheMonitorThread getInstance() {
        if (instance == null) {
            instance = new FIFOCacheMonitorThread();
        }
        return instance;
    }

    public FIFOCacheMonitorThread(List<FIFOCache> cacheList) {
        this.cacheList = cacheList;
    }

    public void registerFIFIOCache(FIFOCache cache) {
        cacheList.add(cache);
    }

    @Override
    public void run() {
        while (true) {
            for (FIFOCache cache: cacheList) {
                if (cache.isReadyToRemove()) {
                    cache.removeBucket();
                }
            }
        }
    }

    @Override
    public String getWorkerName() {
        return "BucketMonitor";
    }

    @Override
    public void setReadyToStop() {
    }

    @Override
    public boolean readyToStop() {
        return false;
    }
}
