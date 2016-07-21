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

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.FilePathWithIndex;

public class BuyerHandler{

	WriteFile buyerfile;
	BufferedReader reader;
	//阻塞队列用于存索引
	LinkedBlockingQueue<IndexItem> indexQueue;
	//DiskHashTable<String, Long> buyerIdSurrKeyIndex = null;
	ConcurrentHashMap<String, DiskHashTable<Integer, List<Long>>> buyerIdIndexList = null;
	List<FilePathWithIndex> buyerFileList = null;
	HashSet<String> buyerAttrList = null;
	int threadIndex = 0;
	CountDownLatch latch = null;
	public SimpleCache rowCache = null;

	public BuyerHandler(List<FilePathWithIndex> buyerFileList, 
			HashSet<String> buyerAttrList,
			ConcurrentHashMap<String, DiskHashTable<Integer, List<Long>>> buyerIdIndexList, 
			int threadIndex, CountDownLatch latch) {
		rowCache = SimpleCache.getInstance();
		this.latch = latch;
		this.buyerFileList = buyerFileList;
		this.buyerAttrList = buyerAttrList;
		//this.buyerIdSurrKeyIndex = buyerIdSurrKeyIndex;
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
					buyerfile.writeLine(file, record, IndexType.BuyerTable);
					record = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// set end signal
		buyerfile.writeLine(null, "end", IndexType.BuyerTable);
		System.out.println("end buyer handling!");
	}

	// buyer表的建索引线程  需要建的索引包括：代理键索引和buyerId的索引
	public class BuyerIndexConstructor implements Runnable {

		String indexFileName = null;
		String dataFileName = null;
		DiskHashTable<Integer, List<Long>> buyerIdHashTable = null;
		boolean isEnd = false;
		HashSet<String> tempAttrList = new HashSet<String>();
		//long surrKey = 1;

		public BuyerIndexConstructor( ) {

		}

		public void run() {

			while( true) {
				IndexItem record = indexQueue.poll();

				if( record != null ) {
					if( record.recordsData.equals("end")) {
						isEnd = true;
						continue;
					}

					if( !record.getIndexFileName().equals(indexFileName)) {
						if( indexFileName == null) {
							// 第一次建立索引文件
							dataFileName = record.getDataFileName();
							indexFileName = record.getIndexFileName();
							buyerIdHashTable = new DiskHashTable<Integer,List<Long>>(
									indexFileName + RaceConfig.buyerIndexFileSuffix ,dataFileName, Long.class);

						}
						else {
							// 保存当前buyerId的索引  并写入索引List
							FilePathWithIndex smallFile = new FilePathWithIndex();

							smallFile.setFilePath(dataFileName);
							//smallFile.setBuyerIdIndex(buyerIdHashTable.writeAllBuckets());
							smallFile.setBuyerIdIndex(0);
							buyerIdIndexList.put(dataFileName, buyerIdHashTable);

							buyerFileList.add(smallFile);

							dataFileName = record.getDataFileName();
							indexFileName = record.getIndexFileName();
							buyerIdHashTable = new DiskHashTable<Integer,List<Long>>(
									indexFileName + RaceConfig.buyerIndexFileSuffix, dataFileName, Long.class);

						}
					}

					Row recordRow = Row
							.createKVMapFromLine(record.recordsData);
					// 添加到缓冲区
					rowCache.putInCache(dataFileName.hashCode() + record.getOffset()
						, record.recordsData, TableName.BuyerTable);
					
					tempAttrList.addAll(recordRow.keySet());			// 添加属性
					String buyerid = recordRow.getKV(RaceConfig.buyerId).valueAsString();

					//buyerIdSurrKeyIndex.put(buyerid, surrKey);					// 建立代理键索引
					buyerIdHashTable.put(buyerid.hashCode(), record.getOffset());
					//surrKey ++;
				}
				else if(isEnd ) {
					// 说明队列为空
					// 将代理键索引写出去  并保存相应数据   将buyerid索引写出去  并保存相应数据
					//buyerIdSurrKeyFile.setFilePath(RaceConfig.buyerSurrFileName);
					//buyerIdSurrKeyFile.setSurrogateIndex(buyerIdSurrKeyIndex.writeAllBuckets());
					synchronized (buyerAttrList) {
						buyerAttrList.addAll(tempAttrList);
					}
					FilePathWithIndex smallFile = new FilePathWithIndex();
					smallFile.setFilePath(dataFileName);
					BucketCachePool.getInstance().removeAllBucket();

					smallFile.setBuyerIdIndex(buyerIdHashTable.writeAllBuckets());

					smallFile.setBuyerIdIndex(0);
					buyerFileList.add(smallFile);
					buyerIdIndexList.put(dataFileName, buyerIdHashTable);
					latch.countDown();
					break;
				}

			}
		}
	}
}
