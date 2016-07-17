package com.alibaba.middleware.handlefile;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;


public class ConstructSystem {

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
		ReadBlockingQueue queues = new ReadBlockingQueue(handlers, 10000);
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
		ReadBlockingQueue queues = new ReadBlockingQueue(handlers, 10000);
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
		ReadBlockingQueue queues = new ReadBlockingQueue(handlers, 10000);
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
		long startTime = System.currentTimeMillis();

		List<String> buyerFiles = new ArrayList<String>();
		buyerFiles.add("buyer_records.txt");
		constructSystem.startBuyerHandling(buyerFiles, 1, 1);
		System.out.println("buyer table time:" + (System.currentTimeMillis() - startTime) / 1000);

		List<String> goodFiles = new ArrayList<String>();
		goodFiles.add("good_records.txt");
		constructSystem.startGoodHandling(goodFiles, 1, 1);
		System.out.println("good table time:" + (System.currentTimeMillis() - startTime) / 1000);

		final List<String> orderFiles = new ArrayList<String>();
		orderFiles.add("order_records1.txt");
		orderFiles.add("order_records2.txt");
		orderFiles.add("order_records3.txt");
		orderFiles.add("order_records4.txt");
		orderFiles.add("order_records5.txt");
		orderFiles.add("order_records6.txt");
		orderFiles.add("order_records7.txt");
		orderFiles.add("order_records8.txt");
		orderFiles.add("order_records9.txt");
		orderFiles.add("order_records10.txt");
		constructSystem.startOrderHandling(orderFiles, 1, 7);
		System.out.println("good table time:" + (System.currentTimeMillis() - startTime) / 1000);

	}
}
