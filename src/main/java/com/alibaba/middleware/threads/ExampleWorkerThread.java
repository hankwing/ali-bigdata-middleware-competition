package com.alibaba.middleware.threads;

/**
 * Created by Jelly on 7/12/16.
 */
public class ExampleWorkerThread extends WorkerThread {

    /**
     * Thread main content. Get out of while loop by interruption
     * */
    @Override
    public void run() {
        while (!Thread.interrupted()) {
            //
        }
    }
}
