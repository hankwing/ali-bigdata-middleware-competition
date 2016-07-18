package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class BuyerHandler{

	AgentMapping agentBuyerMapping;
	WriteFile buyerfile;
	BufferedReader reader;
	//阻塞队列用于存索引
	LinkedBlockingQueue<IndexItem> indexQueue;

	public BuyerHandler(AgentMapping agentBuyerMapping) {
		this.agentBuyerMapping = agentBuyerMapping;
		buyerfile = new WriteFile("buyer/", "buyer_", 10000000);
		indexQueue = new LinkedBlockingQueue<IndexItem>();
	}

	public void handleBuyerRecord(String record){
		String buyerid = Utils.getValueFromRecord(record, "buyerid");
		Integer agentBuyerId = agentBuyerMapping.getValue(buyerid);
		if (agentBuyerId == null) {
			agentBuyerMapping.addEntry(buyerid);
		}
		System.out.println(record.length()+" "+ buyerfile.getOffset());
		//写入文件之前获取索引,放入阻塞队列中，Buyer表中对buyerid键索引
		IndexItem item = new IndexItem(buyerid, buyerfile.getFileName(), buyerfile.getOffset(), IndexType.BUYERFILE_BUYERID);
		try {
			indexQueue.put(item);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		buyerfile.writeLine(record);
	}

	public void handeBuyerFiles(List<String> files){
		System.out.println("start buyer handling!");
		for (String file : files) {
			try {
				reader = new BufferedReader(new FileReader(file));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			String record = null;
			try {
				record = reader.readLine();
				while (record != null) {
					handleBuyerRecord(record);
					record = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		buyerfile.closeFile();
		System.out.println("end buyer handling!");
	}
}