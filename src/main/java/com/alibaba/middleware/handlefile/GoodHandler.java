package com.alibaba.middleware.handlefile;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class GoodHandler implements Runnable{

	AgentMapping agentGoodMapping;
	LinkedBlockingQueue<String> queue;
	CountDownLatch countDownLatch;
	WriteFile goodfile;
	int readers;

	public GoodHandler(AgentMapping agentGoodMapping,
			LinkedBlockingQueue<String> queue,
			CountDownLatch countDownLatch,
			int threadId,
			int readers) {
		this.agentGoodMapping = agentGoodMapping;
		this.queue = queue;
		this.countDownLatch = countDownLatch;
		goodfile = new WriteFile("good/", "good_" + threadId + "_" , 10000000);
		this.readers = readers;
	}

	public synchronized void handleRecord(String record){
		String value = Utils.getValueFromRecord(record, "goodid");
		Integer agentGoodId = agentGoodMapping.getValue(value);
		if (agentGoodId == null) {
			agentGoodMapping.addEntry(value);
		}
		goodfile.writeLine(record);
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
		goodfile.closeFile();
		countDownLatch.countDown();	
	}
}
