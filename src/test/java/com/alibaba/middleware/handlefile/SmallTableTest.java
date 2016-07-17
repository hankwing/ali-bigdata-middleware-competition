package com.alibaba.middleware.handlefile;

import com.alibaba.middleware.threads.ThreadPool;
import com.alibaba.middleware.threads.WorkerThread;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public class SmallTableTest {

    private static ThreadPool threadPool;
    private AgentMapping agentBuyerMapping;
    private AgentMapping agentGoodMapping;

    public SmallTableTest() {
        threadPool = ThreadPool.getInstance();
        agentBuyerMapping = new AgentMapping();
        agentGoodMapping = new AgentMapping();
    }

    public static void main(String[] args) {
        List<String> goodFiles = new ArrayList<String>();
        List<String> buyerFiles = new ArrayList<String>();
        goodFiles.add("good_records_1.txt");
        goodFiles.add("good_records_2.txt");
        goodFiles.add("good_records_3.txt");
        buyerFiles.add("buyer_records_1.txt");
        buyerFiles.add("buyer_records_2.txt");
        buyerFiles.add("buyer_records_3.txt");

        long before = System.currentTimeMillis();
        SmallTableTest smallTableTest = new SmallTableTest();
        smallTableTest.startGoodHandling(goodFiles, 3, 3);
        long middle = System.currentTimeMillis();
        smallTableTest.startBuyerHandling(buyerFiles, 3, 3);
        long end = System.currentTimeMillis();

//        threadPool.shutdown();
        System.out.println("Total time: " + (end - before) + "ms\nGoodHandling time: " + (middle - before) + "ms\nBuyerHandling time: " + (end - middle));
    }

    public void startGoodHandling(List<String> goodfiles, int readers, int handlers){
        System.out.println("start handling goods!");
        long before = System.currentTimeMillis();
        ReadBlockingQueue queues = new ReadBlockingQueue(handlers, 10000);

        for (int i = 0; i < readers; i++) {
            WorkerThread t = new ReadFile(queues, getDealingFiles(goodfiles, i, readers));
//            threadPool.addWorker(t);
        }

        for (int i = 0; i < handlers; i++) {
            LinkedBlockingQueue<String> queue = queues.getBlockQueue(i);
            GoodHandler h = new GoodHandler(agentGoodMapping, queue, i, readers);
//            threadPool.addWorker(h);
        }

        threadPool.startWorkers();
//        threadPool.stopWorkers();
//        threadPool.shutdown();
        long end = System.currentTimeMillis();
        System.out.println("end handling goods! Cost: " + (end - before) + " ms");
    }

    public void startBuyerHandling(List<String> buyerfiles, int readers, int handlers){
        System.out.println("start handling buyers!");
        long before = System.currentTimeMillis();
        ReadBlockingQueue queues = new ReadBlockingQueue(handlers, 10000);

        for (int i = 0; i < readers; i++) {
            WorkerThread t = new ReadFile(queues, getDealingFiles(buyerfiles, i, readers));
            threadPool.addWorker(t);
        }

        for (int i = 0; i < handlers; i++) {
            LinkedBlockingQueue<String> queue = queues.getBlockQueue(i);
            BuyerHandler h = new BuyerHandler(agentBuyerMapping, queue, i, readers);
            threadPool.addWorker(h);
        }

        threadPool.startWorkers();
//        threadPool.stopWorkers();
//        threadPool.shutdown();
        long end = System.currentTimeMillis();
        System.out.println("end handling goods! Cost: " + (end - before) + " ms");
    }

    private List<String> getDealingFiles(List<String> files, int group ,int readers){
        //分给多个读线程
        List<String> list = new ArrayList<String>();
        for (int i = group; i < files.size(); i += readers ) {
            list.add(files.get(i));
        }
        return list;
    }
}
