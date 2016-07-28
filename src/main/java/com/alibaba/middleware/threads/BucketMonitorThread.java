package com.alibaba.middleware.threads;

import com.alibaba.middleware.cache.FIFOCache;

import java.util.ArrayList;
import java.util.List;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public class BucketMonitorThread extends WorkerThread {
    private List<FIFOCache> cacheList;

    public BucketMonitorThread() {
        cacheList = new ArrayList<FIFOCache>();
    }

    public BucketMonitorThread(List<FIFOCache> cacheList) {
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
//                    System.out.println("Remove");
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
