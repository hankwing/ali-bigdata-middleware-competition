package com.alibaba.middleware.handlefile;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class BuyerHandler implements Runnable{

	int threadId;
	AgentMapping agentBuyerMapping;
	CountDownLatch countDownLatch;
	LinkedBlockingQueue<String> queue;
	WriteFile buyerfile;
	int readers;

	public BuyerHandler(AgentMapping agentBuyerMapping,
			LinkedBlockingQueue<String> queue,
			CountDownLatch countDownLatch,
			int threadId,
			int readers) {
		this.agentBuyerMapping = agentBuyerMapping;
		this.queue = queue;
		this.countDownLatch = countDownLatch;
		this.threadId = threadId;
		this.readers = readers;
		buyerfile = new WriteFile("buyer/", "buyer_"+threadId+"_" , 100);
	}

	public synchronized void handleRecord(String record){
		String value = Utils.getValueFromRecord(record, "buyerid");
		Integer agentBuyerId = agentBuyerMapping.getValue(value);
		if (agentBuyerId == null) {
			agentBuyerMapping.addEntry(value);
		}
		buyerfile.writeLine(record);
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (true) {
			try {
				String record = queue.take();
				if (record.equals("END")) {
					readers--;
				}else {
					handleRecord(record);
				}
				if (readers == 0) {
					break;
				}	
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		countDownLatch.countDown();
		buyerfile.closeFile();
	}	
}