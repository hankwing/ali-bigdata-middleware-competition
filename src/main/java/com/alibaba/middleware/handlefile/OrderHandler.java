package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

public class OrderHandler{

	AgentMapping agentBuyerMapping;
	AgentMapping agentGoodMapping;
	HashMap<String, WriteFile> columnFiles;
	WriteFile orderfile;
	BufferedReader reader;
	LinkedBlockingQueue<IndexItem> indexQueue;

	public OrderHandler(AgentMapping agentGoodMapping,
			AgentMapping agentBuyerMapping) {

		this.agentGoodMapping = agentGoodMapping;
		this.agentBuyerMapping = agentBuyerMapping;

		orderfile = new WriteFile("order/", "order_", 1000000);
		columnFiles = new HashMap<String, WriteFile>();

		indexQueue = new LinkedBlockingQueue<IndexItem>();
	}

	//处理每一条记录
	public void handleOrderRecord(String record){	

		String[] kvs = record.split("\t");

		StringBuilder resultBuilder = new StringBuilder(kvs[0]).append("\t");
		String orderid = Utils.getValueFromKV(kvs[0]);

		//获取代理键
		Integer agentGoodId = agentGoodMapping.getValue(Utils.getValueFromKV(kvs[1]));
		Integer agentBuyerId =  agentBuyerMapping.getValue(Utils.getValueFromKV(kvs[2]));

		resultBuilder.append(agentGoodId).append("\t");
		resultBuilder.append(agentBuyerId).append("\t");

		for(int i = 3; i<kvs.length ;i++){
			int p = kvs[i].indexOf(":");
			String key = kvs[i].substring(0 , p);
			String value = kvs[i].substring(p+1);
			if (key.length() == 0 || value.length() == 0) {
				throw new RuntimeException("Bad data:" + record);
			}
			if(Utils.isCanSum(value)) {
				//获取WriteFile,存入相应的WriteFile中
				WriteFile writeFile = columnFiles.get(key);
				if (writeFile == null) {
					writeFile = new WriteFile("cacluate/", key+"_", 10000000);
					columnFiles.put(key, writeFile);
				}
				
				String sumRecord = new String(String.valueOf(agentBuyerId)+":"+ value);
				writeFile.writeLine(sumRecord);
			}
			resultBuilder.append(kvs[i]).append("\t");
		}

		//写入文件之前获取索引,放入阻塞队列中，Order表中对orderid键索引
		IndexItem item = new IndexItem(orderid, orderfile.getFileName(), orderfile.getOffset(), IndexType.ORDERFILE_ORDERID);
		try {
			indexQueue.put(item);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		orderfile.writeLine(resultBuilder.toString());
	}

	public void HandleOrderFiles(List<String> files){
		System.out.println("start order handling!");
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
					handleOrderRecord(record);
					record = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		orderfile.closeFile();
		//关闭ColumnFiles
		Iterator<Entry<String, WriteFile>> itr = columnFiles.entrySet().iterator();
		while (itr.hasNext()) {
			Entry<String, WriteFile> entry = (Entry<String, WriteFile>) itr.next();
			WriteFile writeFile = (WriteFile) entry.getValue();
			writeFile.closeFile();
		}
		System.out.println("end order handling!");
	}
}
