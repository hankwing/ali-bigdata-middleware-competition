package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class GoodHandler{

	AgentMapping agentGoodMapping;
	WriteFile goodfile;
	BufferedReader reader;
	LinkedBlockingQueue<IndexItem> indexQueue;

	public GoodHandler(AgentMapping agentGoodMapping) {
		this.agentGoodMapping = agentGoodMapping;
		goodfile = new WriteFile("good/", "good_", 10000000);
		indexQueue = new LinkedBlockingQueue<IndexItem>();
	}

	private void handleGoodRecord(String record){
		String goodid = Utils.getValueFromRecord(record, "goodid");
		Integer agentGoodId = agentGoodMapping.getValue(goodid);
		if (agentGoodId == null) {
			agentGoodMapping.addEntry(goodid);
		}
		//写入文件之前获取索引,放入阻塞队列中，Good表中对goodid键索引
		IndexItem item = new IndexItem(goodid, goodfile.getFileName(), goodfile.getOffset(), IndexType.GOODFILE_GOODID);
		try {
			indexQueue.put(item);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		goodfile.writeLine(record);
	}

	public void HandleGoodFiles(List<String> files){
		System.out.println("start good handling!");
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
					handleGoodRecord(record);
					record = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		goodfile.closeFile();
		System.out.println("end good handling!");
	}
}
