package com.alibaba.middleware.threads;

import com.alibaba.middleware.conf.RaceConfig;

import java.util.*;
import java.util.concurrent.*;

/**
 * Thread Pool
 * @author Jelly
 */
public class ThreadPool {
    private int workerThreadNum = RaceConfig.workerThreadNum;
    private static ThreadPool instance = null;
    private static ExecutorService workerExe;
    private List<WorkerThread> workerList = new LinkedList<WorkerThread>();

    private ThreadPool() {
        workerExe = Executors.newFixedThreadPool(workerThreadNum);
    }

    public static ThreadPool getInstance() {
        if (instance == null) {
            instance = new ThreadPool();
        }
        return instance;
    }

    public void addWorker(WorkerThread wThread) {
        workerList.add(wThread);
    }

    public void startWorkers() {
        for (WorkerThread t: workerList) {
             workerExe.submit(t);
        }
    }

    public void stopWorkers() {
        for (WorkerThread t: workerList) {
            t.setReadyToStop();
        }
        workerList.clear();
    }

    public void shutdown() {
        workerExe.shutdown();
    }
}
