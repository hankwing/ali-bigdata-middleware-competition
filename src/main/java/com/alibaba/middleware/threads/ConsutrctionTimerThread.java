package com.alibaba.middleware.threads;

import com.alibaba.middleware.handlefile.ConstructSystem;

import java.util.TimerTask;

/**
 * @author Jelly
 */
public class ConsutrctionTimerThread extends TimerTask {
    @Override
    public void run() {
        while (ConstructSystem.lastCountDownLatch.getCount() > 0) {
            ConstructSystem.lastCountDownLatch.countDown();
        }
    }
}
