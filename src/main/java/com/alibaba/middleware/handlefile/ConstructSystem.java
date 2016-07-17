package com.alibaba.middleware.handlefile;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;


public class ConstructSystem {

	ReadBlockingQueue queues;
	CountDownLatch countDownLatch;
	AgentMapping agentBuyerMapping;
	AgentMapping agentGoodMapping;

	public ConstructSystem() {
		agentBuyerMapping = new AgentMapping();
		agentGoodMapping = new AgentMapping();
	}

	private List<String> getDealingFiles(List<String> files, int group ,int readers){
		//分给多个读线程
		List<String> list = new ArrayList<String>();
		for (int i = group; i < files.size(); i += readers ) {
			list.add(files.get(i));
		}
		return list;
	}

	public void startOrderHandling(List<String> orderfiles, int readers, int handlers){
		System.out.println("start handling orders!");
		queues = new ReadBlockingQueue(handlers, 100);
		countDownLatch = new CountDownLatch(readers + handlers);

		for (int i = 0; i < readers; i++) {
			new Thread(new ReadFile(queues,
					countDownLatch, 
					getDealingFiles(orderfiles,i,readers))).start();
		}

		for (int i = 0; i < handlers; i++) {
			LinkedBlockingQueue<String> queue = queues.getBlockQueue(i);
			new Thread(new OrderHandler(agentGoodMapping, 
					agentBuyerMapping, 
					queue,
					countDownLatch,
					i,
					readers)).start();
		}
		try {
			countDownLatch.await();
			//关闭所有ColumnFiles

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("end handling orders!");

	}

	public void startGoodHandling(List<String> goodfiles, int readers, int handlers){
		System.out.println("start handling goods!");
		queues = new ReadBlockingQueue(handlers, 10000);
		countDownLatch = new CountDownLatch(readers + handlers);

		for (int i = 0; i < readers; i++) {
			new Thread(new ReadFile(queues,
					countDownLatch, 
					getDealingFiles(goodfiles,i,readers))).start();
		}

		for (int i = 0; i < handlers; i++) {
			LinkedBlockingQueue<String> queue = queues.getBlockQueue(i);
			new Thread(new GoodHandler(agentGoodMapping, 
					queue, 
					countDownLatch, 
					i,
					readers)).start();
		}

		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("end handling goods!");
	}

	public void startBuyerHandling(List<String> buyerfiles, int readers, int handlers){
		System.out.println("start handling buyers!");
		queues = new ReadBlockingQueue(handlers, 10000);
		countDownLatch = new CountDownLatch(readers + handlers);

		for (int i = 0; i < readers; i++) {
			new Thread(new ReadFile(queues,
					countDownLatch, 
					getDealingFiles(buyerfiles,i,readers))).start();
		}

		for (int i = 0; i < handlers; i++) {
			LinkedBlockingQueue<String> queue = queues.getBlockQueue(i);
			new Thread(new BuyerHandler(agentBuyerMapping, 
					queue, 
					countDownLatch, 
					i,
					readers)).start();
		}
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("end handling buyers!");
	}


	public static void main(String args[]){
		ConstructSystem constructSystem = new ConstructSystem();

		final List<String> buyerFiles = new ArrayList<String>();
		buyerFiles.add("buyer_records1.txt");
		buyerFiles.add("buyer_records2.txt");
		buyerFiles.add("buyer_records3.txt");
		buyerFiles.add("buyer_records4.txt");
		buyerFiles.add("buyer_records5.txt");
		constructSystem.startBuyerHandling(buyerFiles, 1, 1);

		final List<String> goodFiles = new ArrayList<String>();
		goodFiles.add("good_records1.txt");
		goodFiles.add("good_records2.txt");
		goodFiles.add("good_records3.txt");
		goodFiles.add("good_records4.txt");
		goodFiles.add("good_records5.txt");
		constructSystem.startGoodHandling(goodFiles, 1, 1);

		final List<String> orderFiles = new ArrayList<String>();
		orderFiles.add("order_records.txt");
		constructSystem.startOrderHandling(orderFiles, 1, 2);

	}
}
