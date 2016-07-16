package com.alibaba.middleware.threads;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jelly
 */
public class ThreadPoolTest {
    private static ThreadPool threadPool = ThreadPool.getInstance();

    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) {
            ExampleThread exampleThread = new ExampleThread("worker" + i);
            threadPool.addWorker(exampleThread);
        }
        threadPool.startWorkers();
        threadPool.stopWorkers();
        threadPool.shutdown();
    }
}

class ExampleThread extends WorkerThread {
    private String workerName = "ExampleThread";
    private AtomicBoolean isReadyToStop = new AtomicBoolean(true);

    public ExampleThread() {

    }

    public ExampleThread(String workerName) {
        this.workerName = workerName;
    }

    @Override
    public String getWorkerName() {
        return workerName;
    }

    @Override
    public void setReadyToStop() {
        isReadyToStop.set(true);
    }

    @Override
    public boolean readyToStop() {
        return isReadyToStop.get();
    }

    @Override
    public void run() {
        while (!readyToStop()) {
            // DO NOTHING
        }
        System.out.println(getWorkerName()+" stops");
    }
}
