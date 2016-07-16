package com.alibaba.middleware.handlefile;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class OrderHandler implements Runnable{

	AgentMapping agentBuyerMapping;
	AgentMapping agentGoodMapping;
	HashMap<String, WriteFile> columnFiles;
	CountDownLatch countDownLatch;
	LinkedBlockingQueue<String> queue;
	WriteFile orderfile;
	int readers;
	int threadId;

	public OrderHandler(AgentMapping agentGoodMapping,
			AgentMapping agentBuyerMapping,
			LinkedBlockingQueue<String> queue,
			CountDownLatch countDownLatch,
			int threadId,
			int readers) {

		this.agentGoodMapping = agentGoodMapping;
		this.agentBuyerMapping = agentBuyerMapping;
		this.queue = queue;
		this.countDownLatch = countDownLatch;
		this.threadId = threadId;
		this.readers = readers;
		orderfile = new WriteFile("order/", "order_"+ threadId +"_", 1000);
		columnFiles = new HashMap<String, WriteFile>();
	}

	//处理每一条记录
	public synchronized void handleRecord(String record){

		Integer agentGoodId = agentGoodMapping.getValue(Utils.getValueFromRecord(record, "goodid"));
		if (agentGoodId == null) {
			System.out.println("goodid error: " + record);
		}
		Integer agentBuyerId =  agentBuyerMapping.getValue(Utils.getValueFromRecord(record, "buyerid"));
		if (agentBuyerId == null) {
			System.out.println("buyerid error: " + record);
		}
		String[] kvs = record.split("\t");
		String result = new String(kvs[0]+"\t");

		for(int i = 1; i<kvs.length ;i++){
			String str = new String(kvs[i]);
			int p = str.indexOf(":");
			String key = str.substring(0 , p);
			String value = str.substring(p+1);
			if (key.length() == 0 || value.length() == 0) {
				throw new RuntimeException("Bad data:" + record);
			}

			if(Utils.isCanSum(value)) {
				//获取WriteFile,存入相应的WriteFile中
				WriteFile writeFile = columnFiles.get(key);
				if (writeFile == null) {
					writeFile = new WriteFile("cacluate/", "t"+ threadId+"_"+key+"_", 5000);
					columnFiles.put(key, writeFile);
				}

				String sumRecord = new String(String.valueOf(agentBuyerId)+":"+ value);
				writeFile.writeLine(sumRecord);
			}

			if (key.equals("goodid")) {
				result = result + "goodid:" + agentGoodId + "\t";
			} else if (key.equals("buyerid")) {
				result = result + "buyerid:" + agentBuyerId + "\t";
			} else {
				result = result + str + "\t";
			}

		}
		orderfile.writeLine(result);
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
		orderfile.closeFile();

		Iterator<Entry<String, WriteFile>> itr = columnFiles.entrySet().iterator();
		while (itr.hasNext()) {
			Entry<String, WriteFile> entry = (Entry<String, WriteFile>) itr.next();
			WriteFile writeFile = (WriteFile) entry.getValue();
			writeFile.closeFile();
		}
		countDownLatch.countDown();
	}
}
