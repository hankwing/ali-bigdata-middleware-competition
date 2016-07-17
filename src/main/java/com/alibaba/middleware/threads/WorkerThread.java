package com.alibaba.middleware.threads;

/**
 * @author Jelly
 *
 * All threads using {@link ThreadPool} must extend this class. Example see {@link ThreadPoolTest}
 */
public abstract class WorkerThread implements Runnable {

    public abstract String getWorkerName();

    /**
     * set isReadyToStop signal to true
     * */
    public abstract void setReadyToStop();

    /**
     * get isReadyToStop signal
     * */
    public abstract boolean readyToStop();
}