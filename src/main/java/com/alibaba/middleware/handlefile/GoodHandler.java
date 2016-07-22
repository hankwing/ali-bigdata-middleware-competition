package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.conf.RaceConfig.TableName;
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
	int threadIndex = 0;
	CountDownLatch latch = null;
	private SimpleCache rowCache = null;

	public GoodHandler(List<FilePathWithIndex> goodFileList, 
			HashSet<String> goodAttrList,
			ConcurrentHashMap<String, DiskHashTable<Integer, List<Long>>> goodIdIndexList, 
			 int threadIndex,CountDownLatch latch) {
		rowCache = SimpleCache.getInstance();
		this.latch = latch;
		this.goodFileList = goodFileList;
		this.goodAttrList = goodAttrList;
		//this.goodIdSurrKeyIndex = goodIdSurrKeyIndex;
		this.goodIdIndexList = goodIdIndexList;
		this.threadIndex = threadIndex;
		indexQueue = new LinkedBlockingQueue<IndexItem>(RaceConfig.QueueNumber);
		goodfile = new WriteFile(new ArrayList<LinkedBlockingQueue<IndexItem>>(){{add(indexQueue);}},
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
					goodfile.writeLine(file, record, TableName.GoodTable);
					record = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// set end signal
		goodfile.writeLine(null, null, TableName.GoodTable);
		System.out.println("end good handling!");
	}
	
	// good表的建索引线程  需要建的索引包括：代理键索引和goodId的索引
		public class GoodIndexConstructor implements Runnable {

			String indexFileName = null;
			String dataFileName = null;
			DiskHashTable<Integer, List<Long>> goodIdHashTable = null;
			boolean isEnd = false;
			HashSet<String> tempAttrList = new HashSet<String>();
			//long surrKey = 1;
			
			public GoodIndexConstructor( ) {
				
			}

			public void run() {
					
				while( true) {
					IndexItem record = indexQueue.poll();
					
					if( record != null ) {
						if( record.getRecordsData() == null) {
							isEnd = true;
							continue;
						}
						
						if( !record.getIndexFileName().equals(indexFileName)) {
							if( indexFileName == null) {
								// 第一次建立索引文件
								dataFileName = record.getDataFileName();
								indexFileName = record.getIndexFileName();
								goodIdHashTable = new DiskHashTable<Integer,List<Long>>(
										indexFileName + RaceConfig.goodIndexFileSuffix,dataFileName, Long.class);

							}
							else {
								// 保存当前goodId的索引  并写入索引List
								FilePathWithIndex smallFile = new FilePathWithIndex();

								smallFile.setFilePath(dataFileName);
								smallFile.setGoodIdIndex(goodIdHashTable.writeAllBuckets());
								smallFile.setGoodIdIndex(0);
								goodIdIndexList.put(dataFileName, goodIdHashTable);

								goodFileList.add(smallFile);
								
								dataFileName = record.getDataFileName();
								indexFileName = record.getIndexFileName();
								goodIdHashTable = new DiskHashTable<Integer,List<Long>>(
										indexFileName + RaceConfig.goodIndexFileSuffix, dataFileName, Long.class);
								
							}
						}
						Row rowData = Row.createKVMapFromLine(record.getRecordsData());	
						tempAttrList.addAll(rowData.keySet());
						String goodid = rowData.getKV(RaceConfig.goodId).valueAsString();
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

						smallFile.setFilePath(dataFileName);
						smallFile.setGoodIdIndex(goodIdHashTable.writeAllBuckets());

						smallFile.setGoodIdIndex(0);
						BucketCachePool.getInstance().removeAllBucket();
						goodFileList.add(smallFile);
						goodIdIndexList.put(dataFileName, goodIdHashTable);
						latch.countDown();
						break;
					}
					
				}
					
				
			}
		}
}
