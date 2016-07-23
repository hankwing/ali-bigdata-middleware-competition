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
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

public class OrderHandler {

	HashMap<String, WriteFile> columnFiles;

	WriteFile orderfile;
	SmallFileWriter smallFileWriter;
	//文件编号映射，文件序列号
	DataFileMapping orderFileMapping;
	int dataFileSerialNumber;

	BufferedReader reader;
	LinkedBlockingQueue<IndexItem> orderIndexQueue;
	LinkedBlockingQueue<IndexItem> orderBuyerIndexQueue;
	LinkedBlockingQueue<IndexItem> orderGoodIndexQueue;
	ConcurrentHashMap<Integer, DiskHashTable<Long, byte[]>> orderIdIndexList = null;
	ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>> orderBuyerIdIndexList = null;
	ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>> orderGoodIdIndexList = null;
	//ConcurrentHashMap<Integer, List<DiskHashTable<Integer, List<byte[]>>>> orderCountableIndexList = null;
	//DiskHashTable<String, Long> buyerIdSurrKeyIndex = null;
	//DiskHashTable<String, Long> goodIdSurrKeyIndex = null;
	HashSet<String> orderAttrList = null;
	int threadIndex = 0;
	CountDownLatch countDownLatch = null;
	private SimpleCache rowCache = null;
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> orderHandlersList = null;

	public double MEG = Math.pow(1024, 2);
	List<String> smallFiles = new ArrayList<String>();

	public OrderHandler( OrderSystemImpl systemImpl, int thread, CountDownLatch countDownLatch) {
		rowCache = SimpleCache.getInstance();
		this.countDownLatch = countDownLatch;
		this.orderIdIndexList = systemImpl.orderIdIndexList;
		this.orderBuyerIdIndexList = systemImpl.orderBuyerIdIndexList;
		this.orderGoodIdIndexList = systemImpl.orderGoodIdIndexList;
		//this.orderCountableIndexList = systemImpl.orderCountableIndexList;
		this.orderAttrList = systemImpl.orderAttrList;
		this.orderHandlersList = systemImpl.orderHandlersList;
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
		this.orderFileMapping = systemImpl.orderFileMapping;

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
			
			File bf = new File(file);
			if (bf.length() < RaceConfig.smallFileSizeThreathod) {
				smallFiles.add(file);
			}else {
				try {
					// 是大文件
					dataFileSerialNumber = orderFileMapping.addDataFileName(file);
					
					reader = new BufferedReader(new FileReader(file));
					// 建立文件句柄
					LinkedBlockingQueue<RandomAccessFile> handlersQueue = 
							orderHandlersList.get(dataFileSerialNumber);
					if( handlersQueue == null) {
						handlersQueue = new LinkedBlockingQueue<RandomAccessFile>();
						orderHandlersList.put(dataFileSerialNumber, handlersQueue);
					}

					for( int i = 0; i < RaceConfig.fileHandleNumber ; i++) {
						handlersQueue.add(new RandomAccessFile(file, "r"));
					}
					String record = reader.readLine();
					while (record != null) {
						//Utils.getAttrsFromRecords(orderAttrList, record);
						orderfile.writeLine(dataFileSerialNumber, record, TableName.OrderTable);
						record = reader.readLine();
					}
					reader.close();
					
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}


		}

		// set end signal
//		orderfile.writeLine(null, 0, null, TableName.OrderTable);
		// 下面开始处理小文件
		smallFileWriter = new SmallFileWriter(
				orderHandlersList, orderFileMapping,
				new ArrayList<LinkedBlockingQueue<IndexItem>>(){{add(orderIndexQueue);
				add(orderBuyerIndexQueue); add(orderGoodIndexQueue);}}, 
				RaceConfig.storeFolders[threadIndex],
				RaceConfig.orderFileNamePrex);
		//处理小文件
		for(String smallfile:smallFiles){
			try {
				reader = new BufferedReader(new FileReader(smallfile));
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			String record = null;
			try {
				record = reader.readLine();
				while (record != null) {
					//Utils.getAttrsFromRecords(buyerAttrList, record);
					smallFileWriter.writeLine( record, TableName.OrderTable);
					record = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		smallFileWriter.writeLine(null, TableName.OrderTable);
		smallFileWriter.closeFile();
		
		System.out.println("end order handling!");
	}

	// order表的建索引线程 需要建的索引包括：orderid, buyerid, goodid, countable 字段的索引
	public class OrderIndexConstructor implements Runnable {

		String indexFileName = null;
		int fileIndex = 0;
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
								fileIndex = record.getFileIndex();
								switch(indexType) {
								case OrderId:
									idHashTable = new DiskHashTable<Long, byte[]>(
											indexFileName
											+ RaceConfig.orderIndexFileSuffix
											, byte[].class);
									break;
								case OrderBuyerId:
									idHashTable = new DiskHashTable<Integer, List<byte[]>>(
											indexFileName
											+ RaceConfig.orderBuyerIdIndexFileSuffix, List.class);
									break;
								case OrderGoodId:
									idHashTable = new DiskHashTable<Integer, List<byte[]>>(
											indexFileName
											+ RaceConfig.orderGoodIdIndexFileSuffix, List.class);
									break;
								}

							} else {
								switch(indexType) {
								case OrderId:
									// 保存当前goodId的索引 并写入索引List
									idHashTable.writeAllBuckets();
									orderIdIndexList.put(fileIndex, idHashTable);
									fileIndex = record.getFileIndex();
									indexFileName = record.getIndexFileName();
									idHashTable = new DiskHashTable<Long, byte[]>(
											indexFileName
											+ RaceConfig.orderIndexFileSuffix,byte[].class);
									break;
								case OrderBuyerId:
									orderBuyerIdIndexList.put(fileIndex, idHashTable);
									idHashTable.writeAllBuckets();
									fileIndex = record.getFileIndex();
									indexFileName = record.getIndexFileName();
									idHashTable = new DiskHashTable<Integer, List<byte[]>>(
											indexFileName
											+ RaceConfig.orderBuyerIdIndexFileSuffix, List.class);
									break;
								case OrderGoodId:
									orderGoodIdIndexList.put(fileIndex, idHashTable);
									idHashTable.writeAllBuckets();
									fileIndex = record.getFileIndex();
									indexFileName = record.getIndexFileName();
									idHashTable = new DiskHashTable<Integer, List<byte[]>>(
											indexFileName
											+ RaceConfig.orderGoodIdIndexFileSuffix, List.class);
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
							idHashTable.put(buyerIdHashCode, record.getOffset());

							break;
						case OrderGoodId:
							Integer goodIdHashCode = rowData.get(
									RaceConfig.goodId).valueAsString().hashCode();
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
							idHashTable.writeAllBuckets();
							orderIdIndexList.put(fileIndex, idHashTable);
							break;
						case OrderBuyerId:
							idHashTable.writeAllBuckets();
							orderBuyerIdIndexList.put(fileIndex, idHashTable);
							break;
						case OrderGoodId:
							idHashTable.writeAllBuckets();
							orderGoodIdIndexList.put(fileIndex,idHashTable);
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
