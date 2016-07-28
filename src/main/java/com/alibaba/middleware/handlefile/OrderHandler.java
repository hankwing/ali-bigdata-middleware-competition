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
import com.alibaba.middleware.cache.ConcurrentCache;
import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.conf.RaceConfig.IdIndexType;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.handlefile.BuyerHandler.BuyerIndexConstructor;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.BytesKey;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

public class OrderHandler {

	HashMap<String, WriteFile> columnFiles;

	WriteFile orderfile;
	SmallFileWriter smallFileWriter;
	//文件编号映射，文件序列号
	DataFileMapping orderFileMapping;
	DataFileMapping orderIndexMapping;
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
	private ConcurrentCache rowCache = null;
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> orderHandlersList = null;

	public double MEG = Math.pow(1024, 2);
	List<String> smallFiles = new ArrayList<String>();

	public OrderHandler( OrderSystemImpl systemImpl, int thread, CountDownLatch countDownLatch) {
		rowCache = ConcurrentCache.getInstance();
		this.countDownLatch = countDownLatch;
		this.orderIdIndexList = systemImpl.orderIdIndexList;
		this.orderBuyerIdIndexList = systemImpl.orderBuyerIdIndexList;
		this.orderGoodIdIndexList = systemImpl.orderGoodIdIndexList;
		//this.orderCountableIndexList = systemImpl.orderCountableIndexList;
		//this.orderAttrList = systemImpl.orderAttrList;
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
				(int) RaceConfig.maxIndexFileCapacity);

		//文件映射
		this.orderFileMapping = systemImpl.orderFileMapping;
		this.orderIndexMapping = systemImpl.orderIndexMapping;

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
			System.out.println("order file:" + file);
			File bf = new File(file);
			if (bf.length() < RaceConfig.smallFileSizeThreshold) {
				System.out.println("small order file:" + file);
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

		smallFileWriter = new SmallFileWriter(
				orderHandlersList, orderFileMapping,
				new ArrayList<LinkedBlockingQueue<IndexItem>>(){{add(orderIndexQueue);
				add(orderBuyerIndexQueue); add(orderGoodIndexQueue);}}, 
				RaceConfig.storeFolders[threadIndex],
				RaceConfig.orderFileNamePrex);
		//处理小文件
		for(String smallfile:smallFiles) {
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
		//HashSet<String> tempAttrList = new HashSet<String>();
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

								fileIndex = orderIndexMapping.addDataFileName(indexFileName);
								switch(indexType) {
								case OrderId:
									String diskFileName = RaceConfig.storeFolders[threadIndex]
											+ indexFileName.replace("//", "_");
									idHashTable = new DiskHashTable<Long, byte[]>(
											diskFileName
											+ RaceConfig.orderIndexFileSuffix
											, byte[].class, DirectMemoryType.MainSegment);
									break;
								case OrderBuyerId:
									String orderBuyerDiskFileName = RaceConfig.storeFolders[(threadIndex + 1) % 3]
											+ indexFileName.replace("//", "_");
									idHashTable = new DiskHashTable<Integer, List<byte[]>>(
											orderBuyerDiskFileName
											+ RaceConfig.orderBuyerIdIndexFileSuffix, List.class
											,DirectMemoryType.BuyerIdSegment);
									break;
								case OrderGoodId:
									String orderGoodDiskFileName = RaceConfig.storeFolders[(threadIndex + 2) % 3]
											+ indexFileName.replace("//", "_");
									idHashTable = new DiskHashTable<Integer, List<byte[]>>(
											orderGoodDiskFileName
											+ RaceConfig.orderGoodIdIndexFileSuffix, List.class,
											DirectMemoryType.GoodIdSegment);
									break;
								}

							} else {
								switch(indexType) {
								case OrderId:
									// 保存当前goodId的索引 并写入索引List
									idHashTable.writeAllBuckets();
									orderIdIndexList.put(fileIndex, idHashTable);
									indexFileName = record.getIndexFileName();
									String diskFileName = RaceConfig.storeFolders[threadIndex]
											+ indexFileName.replace("//", "_");
									idHashTable = new DiskHashTable<Long, byte[]>(
											diskFileName
											+ RaceConfig.orderIndexFileSuffix,byte[].class,
											DirectMemoryType.MainSegment);
									break;
								case OrderBuyerId:
									orderBuyerIdIndexList.put(fileIndex, idHashTable);
									idHashTable.writeAllBuckets();
									indexFileName = record.getIndexFileName();
									String orderBuyerDiskFileName = RaceConfig.storeFolders[(threadIndex + 1) % 3]
											+ indexFileName.replace("//", "_");
									idHashTable = new DiskHashTable<Integer, List<byte[]>>(
											orderBuyerDiskFileName
											+ RaceConfig.orderBuyerIdIndexFileSuffix, List.class,
											DirectMemoryType.BuyerIdSegment);
									break;
								case OrderGoodId:
									orderGoodIdIndexList.put(fileIndex, idHashTable);
									idHashTable.writeAllBuckets();
									indexFileName = record.getIndexFileName();
									String orderGoodDiskFileName = RaceConfig.storeFolders[(threadIndex + 2) % 3]
											+ indexFileName.replace("//", "_");
									idHashTable = new DiskHashTable<Integer, List<byte[]>>(
											orderGoodDiskFileName
											+ RaceConfig.orderGoodIdIndexFileSuffix, List.class,
											DirectMemoryType.GoodIdSegment);
									break;
								}
								
								fileIndex = orderIndexMapping.addDataFileName(indexFileName);
							}
						}

						switch(indexType) {
						case OrderId:
							//tempAttrList.addAll(rowData.keySet());
							long orderId = Long.parseLong(RecordsUtils.getValueFromLine(
									record.getRecordsData(),RaceConfig.orderId));
							// 将order表的数据放入缓冲区
							//rowCache.putInCache(new BytesKey(record.getOffset()), record.getRecordsData(), TableName.OrderTable);
							idHashTable.put(orderId, record.getOffset());

							break;
						case OrderBuyerId:
							int buyerIdHashCode = RecordsUtils.getValueFromLine(
									record.getRecordsData(),RaceConfig.buyerId).hashCode();
							idHashTable.put(buyerIdHashCode, record.getOffset());

							break;
						case OrderGoodId:
							int goodIdHashCode = RecordsUtils.getValueFromLine(
									record.getRecordsData(),RaceConfig.goodId).hashCode();
							idHashTable.put(goodIdHashCode, record.getOffset());
							break;
						}

					} else if (isEnd) {
						// 保存当前goodId的索引 并写入索引List
						switch(indexType) {
						case OrderId:
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
