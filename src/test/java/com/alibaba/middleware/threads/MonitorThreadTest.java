package com.alibaba.middleware.threads;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jelly
 */
public class MonitorThreadTest {
    static long mb = 1024 * 1024L;

    public static void main(String[] args) {
        ThreadPool threadPool = ThreadPool.getInstance();
        JVMMonitorThread thread = new JVMMonitorThread();
        threadPool.startMonitor(thread);
        try {
            printMemMessage();
            System.out.println("Monitor stopped");
            threadPool.stopMonitor();
        } catch (OutOfMemoryError e) {
            e.printStackTrace();
            System.out.println("Out of memory");
            threadPool.stopMonitor();
        }
    }

    public static void printMemMessage() {
        System.out.println("Max Mem: " + getMaxMem() + " MB");
        System.out.println("Total Mem: " + getTotalMem() + " MB");
        System.out.println("Free Mem: " + getFreeMem() + " MB");
        List<Long> list = new ArrayList<Long>();
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 10000; j++) {
                list.add(new Long(i+j));
            }
//            System.out.println("Round " + i + " Total Mem: " + getTotalMem() + " MB");
//            System.out.println("Round " + i + " Free Mem: " + getFreeMem() + " MB");
        }
        for (int i = 0; i < 100; i++) {
            list.remove(i);
//            System.out.println("List size: " + list.size());
        }
    }

    public static long getMaxMem() {
        return Runtime.getRuntime().maxMemory() / mb;
    }

    public static long getTotalMem() {
        return Runtime.getRuntime().totalMemory() / mb;
    }

    public static long getFreeMem() {
        return Runtime.getRuntime().freeMemory() / mb;
    }
}
