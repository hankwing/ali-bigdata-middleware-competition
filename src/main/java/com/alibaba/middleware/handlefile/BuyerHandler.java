package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

public class BuyerHandler{

	AgentMapping agentBuyerMapping;
	WriteFile buyerfile;
	BufferedReader reader;
	//阻塞队列用于存索引
	LinkedBlockingQueue<IndexItem> indexQueue;
	DiskHashTable<String, Long> buyerIdSurrKeyIndex = null;
	ConcurrentHashMap<String, DiskHashTable<Long, Long>> buyerIdIndexList = null;
	List<FilePathWithIndex> buyerFileList = null;
	HashSet<String> buyerAttrList = null;
	FilePathWithIndex buyerIdSurrKeyFile = null;
	int threadIndex = 0;

	public BuyerHandler(List<FilePathWithIndex> buyerFileList, 
			HashSet<String> buyerAttrList, FilePathWithIndex buyerIdSurrKeyFile, 
			ConcurrentHashMap<String, DiskHashTable<Long, Long>> buyerIdIndexList, 
			DiskHashTable<String, Long> buyerIdSurrKeyIndex, int threadIndex) {
		
		this.buyerFileList = buyerFileList;
		this.buyerAttrList = buyerAttrList;
		this.buyerIdSurrKeyFile = buyerIdSurrKeyFile;
		this.buyerIdSurrKeyIndex = buyerIdSurrKeyIndex;
		this.buyerIdIndexList = buyerIdIndexList;
		this.threadIndex = threadIndex;
		indexQueue = new LinkedBlockingQueue<IndexItem>(RaceConfig.QueueNumber);
		buyerfile = new WriteFile(indexQueue, 
				RaceConfig.storeFolders[threadIndex],
				RaceConfig.buyerFileNamePrex, (int) RaceConfig.smallFileCapacity);
		
	}

	/**
	 * 处理每一行数据
	 * @param files
	 */
	public void handeBuyerFiles(List<String> files){
		System.out.println("start buyer handling!");
		new Thread(new BuyerIndexConstructor( )).start();					// 同时开启建索引线程
		
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
					//Utils.getAttrsFromRecords(buyerAttrList, record);
					buyerfile.writeLine(record, IndexType.BuyerTable);
					record = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// set end signal
		buyerfile.writeLine("end", IndexType.BuyerTable);
		buyerfile.closeFile();
		System.out.println("end buyer handling!");
	}
	
	// buyer表的建索引线程  需要建的索引包括：代理键索引和buyerId的索引
	public class BuyerIndexConstructor implements Runnable {

		String indexFileName = null;
		DiskHashTable<Long, Long> buyerIdHashTable = null;
		boolean isEnd = false;
		long surrKey = 0;
		
		public BuyerIndexConstructor( ) {
			
		}

		public void run() {
			// TODO Auto-generated method stub
				
			while( true) {
				IndexItem record = indexQueue.poll();
				
				if( record != null ) {
					if( record.recordsData.equals("end")) {
						isEnd = true;
						continue;
					}
					
					if( !record.getFileName().equals(indexFileName)) {
						if( indexFileName == null) {
							// 第一次建立索引文件
							indexFileName = record.getFileName();
							buyerIdHashTable = new DiskHashTable<Long,Long>(
									indexFileName + RaceConfig.buyerIndexFileSuffix ,indexFileName, Long.class);
							buyerIdSurrKeyIndex = new DiskHashTable<String,Long>(
									RaceConfig.storeFolders[threadIndex] + 
									RaceConfig.buyerSurrFileName,
									RaceConfig.storeFolders[threadIndex] +
									RaceConfig.buyerSurrFileName, Long.class);
						}
						else {
							// 保存当前buyerId的索引  并写入索引List
							FilePathWithIndex smallFile = new FilePathWithIndex();
							smallFile.setFilePath(indexFileName);
							smallFile.setBuyerIdIndex(buyerIdHashTable.writeAllBuckets());
							buyerIdIndexList.put(indexFileName, buyerIdHashTable);
							buyerFileList.add(smallFile);
							
							indexFileName = record.getFileName();
							buyerIdHashTable = new DiskHashTable<Long,Long>(
									record.getFileName() + RaceConfig.buyerIndexFileSuffix, indexFileName, Long.class);
							
						}
					}
					
					Row recordRow = Row
							.createKVMapFromLine(record.recordsData);
					buyerAttrList.addAll(recordRow.keySet());
					String buyerid = recordRow.getKV(RaceConfig.buyerId).valueAsString();
					buyerIdSurrKeyIndex.put(buyerid, surrKey);					// 建立代理键索引
					buyerIdHashTable.put(surrKey, record.getOffset());
					surrKey ++;
				}
				else if(isEnd ) {
					// 说明队列为空
					// 将代理键索引写出去  并保存相应数据   将buyerid索引写出去  并保存相应数据
					buyerIdSurrKeyFile.setFilePath(RaceConfig.buyerSurrFileName);
					buyerIdSurrKeyFile.setSurrogateIndex(buyerIdSurrKeyIndex.writeAllBuckets());
					
					FilePathWithIndex smallFile = new FilePathWithIndex();
					smallFile.setFilePath(indexFileName);
					smallFile.setBuyerIdIndex(buyerIdHashTable.writeAllBuckets());
					buyerFileList.add(smallFile);				
					buyerIdIndexList.put(indexFileName, buyerIdHashTable);
					break;
				}
				
			}
		}
	}
}
