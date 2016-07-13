package com.alibaba.middleware.threads;

/**
 * @author Jelly
 */
public abstract class WorkerThread implements Runnable {

    public void interrupt() {
        Thread.interrupted();
    }
}