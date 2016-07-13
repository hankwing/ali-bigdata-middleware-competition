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
    private int monitorThreadNum = RaceConfig.monitorThreadNum;
    private static ThreadPool instance = null;
    private static ScheduledExecutorService monitorExe;
    private static ExecutorService workerExe;
    private int initDelay = RaceConfig.monitorInitDelayInMills;
    private int fixedDelay = RaceConfig.monitorFixedDelayInMills;
    private List<JVMMonitorThread> monitorList = new ArrayList<JVMMonitorThread>();
    private List<WorkerThread> workerList = new LinkedList<WorkerThread>();

    private ThreadPool() {
        monitorExe = Executors.newScheduledThreadPool(monitorThreadNum);
        workerExe = Executors.newFixedThreadPool(workerThreadNum);
    }

    public static ThreadPool getInstance() {
        if (instance == null) {
            instance = new ThreadPool();
        }
        return instance;
    }

    public void addMonitor(JVMMonitorThread mThread) {
        monitorList.add(mThread);
    }

    public void addWorker(WorkerThread wThread) {
        workerList.add(wThread);
    }

    public void startMonitors() {
        for (JVMMonitorThread t: monitorList) {
             monitorExe.scheduleAtFixedRate(t, initDelay, fixedDelay, TimeUnit.MILLISECONDS);
        }
    }

    public void startWorkers() {
        for (WorkerThread t: workerList) {
             workerExe.submit(t);
        }
    }

    public void stopMonitors() {
        try {
            monitorExe.awaitTermination(100, TimeUnit.MILLISECONDS);
            monitorExe.shutdown();
            monitorList.clear();
        } catch (InterruptedException e) {
            monitorExe.shutdownNow();
        }
    }

    public void stopWorkers() {
        for (WorkerThread t: workerList) {
            t.interrupt();
        }
        workerExe.shutdown();
        workerList.clear();
    }
}
