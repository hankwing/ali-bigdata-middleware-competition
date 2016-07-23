package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdIndexType;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.handlefile.BuyerHandler.BuyerIndexConstructor;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

import javafx.scene.chart.PieChart.Data;

public class OrderHandler {

	HashMap<String, WriteFile> columnFiles;

	WriteFile orderfile;
	MergeSmallFile mergefile;
	//文件编号映射，文件序列号
	DataFileMapping dataFileMapping;
	int dataFileSerialNumber;

	BufferedReader reader;
	LinkedBlockingQueue<IndexItem> orderIndexQueue;
	LinkedBlockingQueue<IndexItem> orderBuyerIndexQueue;
	LinkedBlockingQueue<IndexItem> orderGoodIndexQueue;
	ConcurrentHashMap<String, DiskHashTable<Long, Long>> orderIdIndexList = null;
	ConcurrentHashMap<String, DiskHashTable<Integer, List<Long>>> orderBuyerIdIndexList = null;
	ConcurrentHashMap<String, DiskHashTable<Integer, List<Long>>> orderGoodIdIndexList = null;
	ConcurrentHashMap<String, List<DiskHashTable<Integer, List<Long>>>> orderCountableIndexList = null;
	//DiskHashTable<String, Long> buyerIdSurrKeyIndex = null;
	//DiskHashTable<String, Long> goodIdSurrKeyIndex = null;
	List<FilePathWithIndex> orderFileList = null;
	HashSet<String> orderAttrList = null;
	int threadIndex = 0;
	CountDownLatch countDownLatch = null;
	private SimpleCache rowCache = null;
	public ConcurrentHashMap<String, LinkedBlockingQueue<RandomAccessFile>> fileHandlersList = null;

	public double MEG = Math.pow(1024, 2);
	List<String> smallFiles = new ArrayList<String>();

	public OrderHandler(
			DataFileMapping dataFileMapping,
			ConcurrentHashMap<String, DiskHashTable<Long, Long>> orderIdIndexList,
			ConcurrentHashMap<String, DiskHashTable<Integer, List<Long>>> orderBuyerIdIndexList,
			ConcurrentHashMap<String, DiskHashTable<Integer, List<Long>>> orderGoodIdIndexList,
			ConcurrentHashMap<String, List<DiskHashTable<Integer, List<Long>>>> orderCountableIndexList,
			List<FilePathWithIndex> orderFileList, HashSet<String> orderAttrList,
			int thread, CountDownLatch countDownLatch,
			ConcurrentHashMap<String, LinkedBlockingQueue<RandomAccessFile>> fileHandlersList) {
		rowCache = SimpleCache.getInstance();
		this.countDownLatch = countDownLatch;
		this.orderIdIndexList = orderIdIndexList;
		this.orderBuyerIdIndexList = orderBuyerIdIndexList;
		this.orderGoodIdIndexList = orderGoodIdIndexList;
		this.orderCountableIndexList = orderCountableIndexList;
		this.orderFileList = orderFileList;
		this.orderAttrList = orderAttrList;
		this.fileHandlersList = fileHandlersList;
		//this.buyerIdSurrKeyIndex = buyerIdSurrKeyIndex;
		//this.goodIdSurrKeyIndex = goodIdSurrKeyIndex;
		threadIndex = thread;
		orderIndexQueue = new LinkedBlockingQueue<IndexItem>(RaceConfig.QueueNumber);
		orderBuyerIndexQueue = new LinkedBlockingQueue<IndexItem>(RaceConfig.QueueNumber);
		orderGoodIndexQueue = new LinkedBlockingQueue<IndexItem>(RaceConfig.QueueNumber);
		orderfile = new WriteFile(new ArrayList<LinkedBlockingQueue<IndexItem>>(){{add(orderIndexQueue);
		add(orderBuyerIndexQueue); add(orderGoodIndexQueue);}},
				RaceConfig.storeFolders[threadIndex],
				RaceConfig.orderFileNamePrex,
				(int) RaceConfig.smallFileCapacity);

		//文件映射
		this.dataFileMapping = dataFileMapping;

	}

	public void HandleOrderFiles(List<String> files) {
		System.out.println("start order handling!");
		// 建orderId的索引
		new Thread(new OrderIndexConstructor(IndexType.OrderId, orderIndexQueue)).start();	
		// 建order表里的buyerId索引
		new Thread(new OrderIndexConstructor(IndexType.OrderBuyerId, orderBuyerIndexQueue)).start();
		// 建order表里的goodid索引
		new Thread(new OrderIndexConstructor(IndexType.OrderGoodId, orderGoodIndexQueue)).start();
		for (String file : files) {

			dataFileMapping.addDataFile(file);
			dataFileSerialNumber = dataFileMapping.getDataFileSerialNumber();

			File bf = new File(file);
			if (bf.length() < (long)(100*MEG)) {
				smallFiles.add(file);
			}else {
				try {
					reader = new BufferedReader(new FileReader(file));
					// 建立文件句柄
					LinkedBlockingQueue<RandomAccessFile> handlersQueue = fileHandlersList.get(file);
					if( handlersQueue == null) {
						handlersQueue = new LinkedBlockingQueue<RandomAccessFile>();
						fileHandlersList.put(file, handlersQueue);
					}

					for( int i = 0; i < RaceConfig.fileHandleNumber ; i++) {
						handlersQueue.add(new RandomAccessFile(file, "r"));
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				String record = null;
				try {
					record = reader.readLine();
					while (record != null) {
						//Utils.getAttrsFromRecords(orderAttrList, record);
						orderfile.writeLine(file, dataFileSerialNumber, record, TableName.OrderTable);
						record = reader.readLine();
					}
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}


		}

		// set end signal
		orderfile.writeLine(null, 0, null, TableName.OrderTable);
		System.out.println("end order handling!");
	}

	// order表的建索引线程 需要建的索引包括：orderid, buyerid, goodid, countable 字段的索引
	public class OrderIndexConstructor implements Runnable {

		String indexFileName = null;
		String dataFileName = null;
		DiskHashTable idHashTable = null;
		boolean isEnd = false;
		HashSet<String> tempAttrList = new HashSet<String>();
		IndexType indexType = null;
		LinkedBlockingQueue<IndexItem> indexQueue;

		public OrderIndexConstructor( IndexType indexType, LinkedBlockingQueue<IndexItem> indexQueue) {
			this.indexType = indexType;
			this.indexQueue = indexQueue;

		}

		@SuppressWarnings("unchecked")
		public void run() {
			try {
				while (true) {
					IndexItem record = indexQueue.poll();
					if (record != null) {
						if (record.getRecordsData() == null) {
							isEnd = true;
							continue;
						}

						if (!record.getIndexFileName().equals(indexFileName)) {
							if (indexFileName == null) {
								// 第一次建立索引文件
								indexFileName = record.getIndexFileName();
								dataFileName = record.getDataFileName();
								switch(indexType) {
								case OrderId:
									idHashTable = new DiskHashTable<Long, Long>(
											indexFileName
											+ RaceConfig.orderIndexFileSuffix,
											dataFileName, Long.class);
									break;
								case OrderBuyerId:
									idHashTable = new DiskHashTable<Integer, List<Long>>(
											indexFileName
											+ RaceConfig.orderBuyerIdIndexFileSuffix,
											dataFileName, List.class);
									break;
								case OrderGoodId:
									idHashTable = new DiskHashTable<Integer, List<Long>>(
											indexFileName
											+ RaceConfig.orderGoodIdIndexFileSuffix,
											dataFileName, List.class);
									break;
								}

							} else {
								switch(indexType) {
								case OrderId:
									// 保存当前goodId的索引 并写入索引List
									FilePathWithIndex smallFile = new FilePathWithIndex();
									smallFile.setFilePath(dataFileName);
									smallFile.setOrderIdIndex(idHashTable
											.writeAllBuckets());
									orderFileList.add(smallFile);
									orderIdIndexList.put(dataFileName, idHashTable);
									dataFileName = record.getDataFileName();
									indexFileName = record.getIndexFileName();
									idHashTable = new DiskHashTable<Long, Long>(
											indexFileName
											+ RaceConfig.orderIndexFileSuffix,
											dataFileName, Long.class);
									break;
								case OrderBuyerId:
									orderBuyerIdIndexList.put(dataFileName, idHashTable);
									idHashTable.writeAllBuckets();
									dataFileName = record.getDataFileName();
									indexFileName = record.getIndexFileName();
									idHashTable = new DiskHashTable<Integer, List<Long>>(
											indexFileName
											+ RaceConfig.orderBuyerIdIndexFileSuffix,
											dataFileName, List.class);
									break;
								case OrderGoodId:
									orderGoodIdIndexList.put(dataFileName, idHashTable);
									idHashTable.writeAllBuckets();
									dataFileName = record.getDataFileName();
									indexFileName = record.getIndexFileName();
									idHashTable = new DiskHashTable<Integer, List<Long>>(
											indexFileName
											+ RaceConfig.orderGoodIdIndexFileSuffix,
											dataFileName, List.class);
									break;
								}	
							}
						}

						Row rowData = Row.createKVMapFromLine(record.getRecordsData());
						switch(indexType) {
						case OrderId:
							tempAttrList.addAll(rowData.keySet());
							long orderId = rowData.get(RaceConfig.orderId).valueAsLong();
							// 将order表的数据放入缓冲区
							//rowCache.putInCache(orderId, record.getRecordsData(), TableName.OrderTable);
							idHashTable.put(orderId, record.getOffset());

							break;
						case OrderBuyerId:
							Integer buyerIdHashCode = rowData.get(
									RaceConfig.buyerId).valueAsString().hashCode();
							// 将buyerid对应的orderid放入缓冲区
							//rowCache.putInIdCache(buyerIdHashCode, 
							//		rowData.getKV(RaceConfig.orderId).valueAsLong(), 
							//		IdIndexType.BuyerIdToOrderId);
							idHashTable.put(buyerIdHashCode, record.getOffset());

							break;
						case OrderGoodId:
							Integer goodIdHashCode = rowData.get(
									RaceConfig.goodId).valueAsString().hashCode();
							// 将goodid对应的orderid放入缓冲区
							//rowCache.putInIdCache(goodIdHashCode, 
							//		rowData.getKV(RaceConfig.orderId).valueAsLong(),
							//		IdIndexType.GoodIdToOrderId);
							idHashTable.put(goodIdHashCode, record.getOffset());
							break;
						}

					} else if (isEnd) {
						// 保存当前goodId的索引 并写入索引List
						switch(indexType) {
						case OrderId:
							synchronized (orderAttrList) {
								orderAttrList.addAll(tempAttrList);
							}
							FilePathWithIndex smallFile = new FilePathWithIndex();
							smallFile.setFilePath(dataFileName);
							smallFile.setOrderIdIndex(idHashTable
									.writeAllBuckets());
							orderFileList.add(smallFile);
							orderIdIndexList.put(dataFileName, idHashTable);
							break;
						case OrderBuyerId:
							idHashTable.writeAllBuckets();
							orderBuyerIdIndexList.put(dataFileName, idHashTable);
							break;
						case OrderGoodId:
							idHashTable.writeAllBuckets();
							orderGoodIdIndexList.put(dataFileName,idHashTable);
							break;
						}	
						BucketCachePool.getInstance().removeAllBucket();

						countDownLatch.countDown();
						break;

					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
}
