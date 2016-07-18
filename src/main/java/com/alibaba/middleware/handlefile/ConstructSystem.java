package com.alibaba.middleware.handlefile;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;


public class ConstructSystem {

	//代理映射表
	AgentMapping agentBuyerMapping;
	AgentMapping agentGoodMapping;

	class BuyerRun implements Runnable{
		CountDownLatch countDownLatch;
		List<String> files;
		public BuyerRun(CountDownLatch countDownLatch, List<String> files) {
			this.countDownLatch = countDownLatch;
			this.files = files;
		}
		public void run() {
			// TODO Auto-generated method stub
			
			BuyerHandler buyerHandler = new BuyerHandler(agentBuyerMapping);
			buyerHandler.handeBuyerFiles(files);
			countDownLatch.countDown();
		}
	}

	class GoodRun implements Runnable{
		CountDownLatch countDownLatch;
		List<String> files;
		public GoodRun(CountDownLatch countDownLatch, List<String> files) {
			this.countDownLatch = countDownLatch;
			this.files = files;
		}
		public void run() {
			// TODO Auto-generated method stub
			GoodHandler goodHandler = new GoodHandler(agentGoodMapping);
			goodHandler.HandleGoodFiles(files);
			countDownLatch.countDown();
		}
	}

	class OrderRun implements Runnable{
		CountDownLatch countDownLatch;
		List<String> files;
		public OrderRun(CountDownLatch countDownLatch, List<String> files) {
			this.countDownLatch = countDownLatch;
			this.files = files;
		}
		public void run() {
			// TODO Auto-generated method stub
			OrderHandler orderHandler = new OrderHandler(agentGoodMapping, agentBuyerMapping);
			orderHandler.HandleOrderFiles(files);
			countDownLatch.countDown();
		}
	}

	public ConstructSystem() {
		agentBuyerMapping = new AgentMapping();
		agentGoodMapping = new AgentMapping();
	}

	public void startHandling(List<String> buyerfiles,List<String> goodfiles,List<String> orderfiles,int threadNum){
		CountDownLatch countDownLatch;
		try {
			countDownLatch = new CountDownLatch(threadNum);
			for (int i = 0; i < threadNum; i++) {
				final List<String> files = getGroupFiles(buyerfiles,i,threadNum);
				new Thread(new BuyerRun(countDownLatch, files)).start();;
			}
			countDownLatch.await();


			countDownLatch = new CountDownLatch(threadNum);
			for (int i = 0; i < threadNum; i++) {
				final List<String> files = getGroupFiles(goodfiles, i, threadNum);
				new Thread(new GoodRun(countDownLatch, files)).start();
			}
			countDownLatch.await();

			countDownLatch = new CountDownLatch(threadNum);
			for (int i = 0; i < threadNum; i++) {
				final List<String> files = getGroupFiles(orderfiles, i, threadNum);
				new Thread(new OrderRun(countDownLatch, files)).start();
			}
			countDownLatch.await();
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private List<String> getGroupFiles(List<String> files, int group ,int readers){
		//分给多个读线程
		List<String> list = new ArrayList<String>();
		for (int i = group; i < files.size(); i += readers ) {
			list.add(files.get(i));
		}
		return list;
	}


	public static void main(String args[]){

		long startTime = System.currentTimeMillis();
		List<String> buyerfiles = new ArrayList<String>();
		buyerfiles.add("buyer_records.txt");
		List<String> goodfiles = new ArrayList<String>();
		goodfiles.add("good_records.txt");
		List<String> orderfiles = new ArrayList<String>();
		orderfiles.add("order_records.txt");

		ConstructSystem constructSystem = new ConstructSystem();
		constructSystem.startHandling(buyerfiles, goodfiles, orderfiles, 3);

		System.out.println("order table time:" + (System.currentTimeMillis() - startTime) / 1000);
	}
}
