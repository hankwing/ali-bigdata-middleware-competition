package com.alibaba.middleware.threads;

import com.alibaba.middleware.cache.FIFOCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jelly
 */
public class FIFOCacheMonitorThread extends WorkerThread {
    private List<FIFOCache> cacheList;
    private static FIFOCacheMonitorThread instance = null;
    private AtomicBoolean isReadyToStop = new AtomicBoolean(false);

    private FIFOCacheMonitorThread() {
        cacheList = new CopyOnWriteArrayList<FIFOCache>();
    }

    public static FIFOCacheMonitorThread getInstance() {
        if (instance == null) {
            instance = new FIFOCacheMonitorThread();
        }
        return instance;
    }

    public void registerFIFIOCache(FIFOCache cache) {
        cacheList.add(cache);
    }

    @Override
    public void run() {
    	try {
	        while (true) {
                if (readyToStop()) {
                    System.out.println("Stop fifo monitor");
                    break;
                }
	            for (FIFOCache cache: cacheList) {
	                if (cache.isReadyToRemove()) {
	                    cache.removeBucket();
	                }
	                else {
						Thread.sleep(100);
	                }
	            }
	        }
    	} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
    }

    @Override
    public String getWorkerName() {
        return "BucketMonitor";
    }

    @Override
    public void setReadyToStop() {
        isReadyToStop.set(true);
    }

    @Override
    public boolean readyToStop() {
        return isReadyToStop.get();
    }
}
