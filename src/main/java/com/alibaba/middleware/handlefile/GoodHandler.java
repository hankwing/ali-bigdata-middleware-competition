package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.handlefile.BuyerHandler.BuyerIndexConstructor;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

public class GoodHandler{

	WriteFile goodfile;
	BufferedReader reader;
	LinkedBlockingQueue<IndexItem> indexQueue;

	//DiskHashTable<String, Long> goodIdSurrKeyIndex = null;
	ConcurrentHashMap<String, DiskHashTable<Integer, List<Long>>> goodIdIndexList = null;
	List<FilePathWithIndex> goodFileList = null;
	HashSet<String> goodAttrList = null;
	FilePathWithIndex goodIdSurrKeyFile = null;
	int threadIndex = 0;
	CountDownLatch latch = null;

	public GoodHandler(List<FilePathWithIndex> goodFileList, 
			HashSet<String> goodAttrList, FilePathWithIndex goodIdSurrKeyFile, 
			ConcurrentHashMap<String, DiskHashTable<Integer, List<Long>>> goodIdIndexList, 
			 int threadIndex,CountDownLatch latch) {
		this.latch = latch;
		this.goodFileList = goodFileList;
		this.goodAttrList = goodAttrList;
		this.goodIdSurrKeyFile = goodIdSurrKeyFile;
		//this.goodIdSurrKeyIndex = goodIdSurrKeyIndex;
		this.goodIdIndexList = goodIdIndexList;
		this.threadIndex = threadIndex;
		indexQueue = new LinkedBlockingQueue<IndexItem>(RaceConfig.QueueNumber);
		goodfile = new WriteFile(indexQueue,
				RaceConfig.storeFolders[threadIndex], 
				RaceConfig.goodFileNamePrex, (int) RaceConfig.smallFileCapacity);
	}

	public void HandleGoodFiles(List<String> files){
		System.out.println("start good handling!");
		new Thread(new GoodIndexConstructor()).start();					// 同时开启建索引线程
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
					//Utils.getAttrsFromRecords(goodAttrList, record);
					goodfile.writeLine(record, IndexType.GoodTable);
					record = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// set end signal
		goodfile.writeLine("end",IndexType.GoodTable);
		goodfile.closeFile();
		System.out.println("end good handling!");
	}
	
	// good表的建索引线程  需要建的索引包括：代理键索引和goodId的索引
		public class GoodIndexConstructor implements Runnable {

			String indexFileName = null;
			DiskHashTable<Integer, List<Long>> goodIdHashTable = null;
			boolean isEnd = false;
			HashSet<String> tempAttrList = new HashSet<String>();
			//long surrKey = 1;
			
			public GoodIndexConstructor( ) {
				
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
								goodIdHashTable = new DiskHashTable<Integer,List<Long>>(
										indexFileName + RaceConfig.goodIndexFileSuffix,indexFileName, Long.class);

							}
							else {
								// 保存当前goodId的索引  并写入索引List
								FilePathWithIndex smallFile = new FilePathWithIndex();
								smallFile.setFilePath(indexFileName);
								smallFile.setGoodIdIndex(goodIdHashTable.writeAllBuckets());
								goodIdIndexList.put(indexFileName, goodIdHashTable);
								goodFileList.add(smallFile);
								
								indexFileName = record.getFileName();
								goodIdHashTable = new DiskHashTable<Integer,List<Long>>(
										record.getFileName() + RaceConfig.goodIndexFileSuffix, indexFileName, Long.class);
								
							}
						}
						Row recordRow = Row
								.createKVMapFromLine(record.recordsData);
						tempAttrList.addAll(recordRow.keySet());
						String goodid = recordRow.getKV(RaceConfig.goodId).valueAsString();
						
						//goodIdSurrKeyIndex.put(goodid, surrKey);					// 建立代理键索引
						goodIdHashTable.put(goodid.hashCode(), record.getOffset());
						//surrKey ++;
					}
					else if(isEnd ) {
						// 说明队列为空
						// 将代理键索引写出去  并保存相应数据   将gooderid索引写出去  并保存相应数据
						//goodIdSurrKeyFile.setFilePath(RaceConfig.goodSurrFileName);
						//goodIdSurrKeyFile.setSurrogateIndex(goodIdSurrKeyIndex.writeAllBuckets());
						synchronized (goodAttrList) {
							goodAttrList.addAll(tempAttrList);
				        }
						FilePathWithIndex smallFile = new FilePathWithIndex();
						smallFile.setFilePath(indexFileName);
						smallFile.setGoodIdIndex(goodIdHashTable.writeAllBuckets());
						goodFileList.add(smallFile);
						goodIdIndexList.put(indexFileName, goodIdHashTable);
						latch.countDown();
						break;
					}
					
				}
					
				
			}
		}
}
