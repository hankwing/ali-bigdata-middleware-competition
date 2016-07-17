package com.alibaba.middleware.threads;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public abstract class SchedulerThread implements Runnable {

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
