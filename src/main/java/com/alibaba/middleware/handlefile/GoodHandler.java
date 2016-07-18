package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.conf.RaceConfig;

public class GoodHandler{

	AgentMapping agentGoodMapping;
	WriteFile goodfile;
	BufferedReader reader;
	LinkedBlockingQueue<IndexItem> indexQueue;

	public GoodHandler(AgentMapping agentGoodMapping, int threadid) {
		this.agentGoodMapping = agentGoodMapping;
		goodfile = new WriteFile("buildfiles/good/", "good_"+threadid+"_", RaceConfig.goodFileCapacity);
		indexQueue = new LinkedBlockingQueue<IndexItem>();
	}

	private void handleGoodRecord(String record){
		String goodid = Utils.getValueFromRecord(record, "goodid");
		Long agentGoodId = agentGoodMapping.getValue(goodid);
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
