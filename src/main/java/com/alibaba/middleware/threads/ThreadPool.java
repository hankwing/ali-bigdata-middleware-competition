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
    private static ScheduledExecutorService monitorExe;
    private static ExecutorService workerExe;
    private static ExecutorService queryExe;
    private int initDelay = RaceConfig.monitorInitDelayInMills;
    private int fixedDelay = RaceConfig.monitorFixedDelayInMills;
    private List<SchedulerThread> monitorList = new ArrayList();
    private List<WorkerThread> workerList = new LinkedList<WorkerThread>();

    private ThreadPool() {
        workerExe = Executors.newFixedThreadPool(workerThreadNum);
        monitorExe = Executors.newScheduledThreadPool(RaceConfig.monitorThreadNum);
        queryExe = Executors.newFixedThreadPool(RaceConfig.queryThreadNum);
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

    public void addMonitor(SchedulerThread jvmThread) {
        monitorList.add(jvmThread);
    }

    public ExecutorService getQueryExe() {
        if (!queryExe.isTerminated()) {
            return queryExe;
        }
        return null;
    }

    public void startWorkers() {
        for (WorkerThread t: workerList) {
             workerExe.execute(t);
        }
    }

    public void startMonitors() {
        for (SchedulerThread t: monitorList) {
            monitorExe.scheduleAtFixedRate(t, initDelay, fixedDelay, TimeUnit.MILLISECONDS);
        }
    }

    public void stopWorkers() {
        for (WorkerThread t: workerList) {
            t.setReadyToStop();
        }
        workerList.clear();
    }

    public void stopMonitors() {
        for (SchedulerThread t: monitorList) {
            t.setReadyToStop();
        }
        monitorList.clear();
    }

    public void shutdown() {
        workerExe.shutdown();
        monitorExe.shutdown();
    }
}
