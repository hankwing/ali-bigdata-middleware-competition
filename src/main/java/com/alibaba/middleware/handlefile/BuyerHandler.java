package com.alibaba.middleware.handlefile;

import com.alibaba.middleware.threads.WorkerThread;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class BuyerHandler extends WorkerThread {

	int threadId;
	AgentMapping agentBuyerMapping;
//	CountDownLatch countDownLatch;
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
//		this.countDownLatch = countDownLatch;
		this.threadId = threadId;
		this.readers = readers;
		buyerfile = new WriteFile("buyer/", "buyer_"+threadId+"_" , 10000000);
	}

	public BuyerHandler(AgentMapping agentBuyerMapping,
						LinkedBlockingQueue<String> queue,
						int threadId,
						int readers) {
		this.agentBuyerMapping = agentBuyerMapping;
		this.queue = queue;
		this.threadId = threadId;
		this.readers = readers;
		buyerfile = new WriteFile("buyer/", "buyer_"+threadId+"_" , 10000000);
	}

	public void handleRecord(String record){
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
//		countDownLatch.countDown();
		buyerfile.closeFile();
	}

	@Override
	public String getWorkerName() {
		return "BuyerHandler";
	}

	@Override
	public void setReadyToStop() {

	}

	@Override
	public boolean readyToStop() {
		return false;
	}
}