package com.alibaba.middleware.threads;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jelly
 */
public class JVMMonitorThread extends MonitorThread {
    private final long mb = 1024 * 1024L;
    private long maxMem = getMaxMem();
    private AtomicInteger gcHintCounter = new AtomicInteger(0);
    private final int gcHintThreshold = 2;
    private final float freeFactor = 0.2f;

    public JVMMonitorThread() {
    }

    @Override
    public void run() {
        if (getFreeMem() < (maxMem * freeFactor)) {
            if (gcHintCounter.get() < gcHintThreshold) {
                Runtime.getRuntime().gc();
                gcHintCounter.getAndAdd(1);
            } else {
                // TODO call buffer manager
                System.out.println("Buffer clean up");
                gcHintCounter.set(0);
            }
        } else {
//            System.out.println(getFreeMem());
        }
    }

    public long getMaxMem() {
        return Runtime.getRuntime().maxMemory() / mb;
    }

    public long getTotalMem() {
        return Runtime.getRuntime().totalMemory() / mb;
    }

    public long getFreeMem() {
        return Runtime.getRuntime().freeMemory() / mb;
    }
}
