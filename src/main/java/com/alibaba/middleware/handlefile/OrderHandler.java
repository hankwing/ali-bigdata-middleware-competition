package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.handlefile.BuyerHandler.BuyerIndexConstructor;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

public class OrderHandler {

	HashMap<String, WriteFile> columnFiles;
	WriteFile orderfile;
	BufferedReader reader;
	LinkedBlockingQueue<IndexItem> indexQueue;
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

	public OrderHandler(
			ConcurrentHashMap<String, DiskHashTable<Long, Long>> orderIdIndexList,
			ConcurrentHashMap<String, DiskHashTable<Integer, List<Long>>> orderBuyerIdIndexList,
			ConcurrentHashMap<String, DiskHashTable<Integer, List<Long>>> orderGoodIdIndexList,
			ConcurrentHashMap<String, List<DiskHashTable<Integer, List<Long>>>> orderCountableIndexList,
			List<FilePathWithIndex> orderFileList, HashSet<String> orderAttrList,
			 int thread, CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
		this.orderIdIndexList = orderIdIndexList;
		this.orderBuyerIdIndexList = orderBuyerIdIndexList;
		this.orderGoodIdIndexList = orderGoodIdIndexList;
		this.orderCountableIndexList = orderCountableIndexList;
		this.orderFileList = orderFileList;
		this.orderAttrList = orderAttrList;
		//this.buyerIdSurrKeyIndex = buyerIdSurrKeyIndex;
		//this.goodIdSurrKeyIndex = goodIdSurrKeyIndex;
		threadIndex = thread;
		indexQueue = new LinkedBlockingQueue<IndexItem>(RaceConfig.QueueNumber);
		orderfile = new WriteFile(indexQueue,
				RaceConfig.storeFolders[threadIndex],
				RaceConfig.orderFileNamePrex,
				(int) RaceConfig.smallFileCapacity);
	}

	public void HandleOrderFiles(List<String> files) {
		System.out.println("start order handling!");
		new Thread(new GoodIndexConstructor( )).start();					// 同时开启建索引线程
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
					//Utils.getAttrsFromRecords(orderAttrList, record);
					orderfile.writeLine(record, IndexType.OrderTable);
					record = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// set end signal
		orderfile.writeLine("end", IndexType.OrderTable);
		orderfile.closeFile();
		System.out.println("end order handling!");
	}

	// order表的建索引线程 需要建的索引包括：orderid, buyerid, goodid, countable 字段的索引
	public class GoodIndexConstructor implements Runnable {

		String indexFileName = null;
		DiskHashTable<Long, Long> orderIdHashTable = null;
		DiskHashTable<Integer, List<Long>> orderBuyerIdHashTable = null;
		DiskHashTable<Integer, List<Long>> orderGoodIdHashTable = null;
		boolean isEnd = false;
		HashSet<String> tempAttrList = new HashSet<String>();

		public GoodIndexConstructor() {

		}

		public void run() {
			// TODO Auto-generated method stub
			try {
				while (true) {
					IndexItem record = indexQueue.poll();
					if (record != null) {
						if (record.recordsData.equals("end")) {
							isEnd = true;
							continue;
						}

						if (!record.getFileName().equals(indexFileName)) {
							if (indexFileName == null) {
								// 第一次建立索引文件
								indexFileName = record.getFileName();
								orderIdHashTable = new DiskHashTable<Long, Long>(
										indexFileName
												+ RaceConfig.orderIndexFileSuffix,
										indexFileName, Long.class);
								orderBuyerIdHashTable = new DiskHashTable<Integer, List<Long>>(
										indexFileName
												+ RaceConfig.orderIndexFileSuffix,
										indexFileName, List.class);
								orderGoodIdHashTable = new DiskHashTable<Integer, List<Long>>(
										indexFileName
												+ RaceConfig.orderIndexFileSuffix,
										indexFileName, List.class);

							} else {
								// 保存当前goodId的索引 并写入索引List
								FilePathWithIndex smallFile = new FilePathWithIndex();
								smallFile.setFilePath(indexFileName);
								// buyerIdIndexList.put(indexFileName,
								// buyerIdHashTable);
								smallFile.setOrderIdIndex(orderIdHashTable
										.writeAllBuckets());
								smallFile
										.setOrderBuyerIdIndex(orderBuyerIdHashTable
												.writeAllBuckets());
								smallFile
										.setOrderGoodIdIndex(orderGoodIdHashTable
												.writeAllBuckets());

								orderIdIndexList.put(indexFileName,
										orderIdHashTable);
								orderBuyerIdIndexList.put(indexFileName,
										orderBuyerIdHashTable);
								orderGoodIdIndexList.put(indexFileName,
										orderGoodIdHashTable);
								orderFileList.add(smallFile);

								indexFileName = record.getFileName();
								orderIdHashTable = new DiskHashTable<Long, Long>(
										indexFileName
												+ RaceConfig.orderIndexFileSuffix,
										indexFileName, Long.class);
								orderBuyerIdHashTable = new DiskHashTable<Integer, List<Long>>(
										indexFileName
												+ RaceConfig.orderIndexFileSuffix,
										indexFileName, List.class);
								orderGoodIdHashTable = new DiskHashTable<Integer, List<Long>>(
										indexFileName
												+ RaceConfig.orderIndexFileSuffix,
										indexFileName, List.class);

							}
						}

						Row recordRow = Row
								.createKVMapFromLine(record.recordsData);
						tempAttrList.addAll(recordRow.keySet());
						long orderid = recordRow.get(RaceConfig.orderId)
								.valueAsLong();

						// 获取代理键
						/*long agentBuyerId = buyerIdSurrKeyIndex
								.get(recordRow.get(RaceConfig.buyerId).valueAsString()).get(0);
						long agentGoodId = goodIdSurrKeyIndex.get(
								recordRow.get(RaceConfig.goodId).valueAsString())
								.get(0);*/

						// 建立三个索引  buyerid 和 goodid 的hashcode当作代理键
						orderIdHashTable.put(orderid, record.getOffset());
						orderBuyerIdHashTable.put(
								recordRow.get(RaceConfig.buyerId).valueAsString().hashCode(),
								record.getOffset());
						orderGoodIdHashTable.put(
								recordRow.get(RaceConfig.goodId).valueAsString().hashCode(),
								record.getOffset());

					} else if (isEnd) {
						// 保存当前goodId的索引 并写入索引List
						synchronized (orderAttrList) {
							orderAttrList.addAll(tempAttrList);
				        }
						FilePathWithIndex smallFile = new FilePathWithIndex();
						smallFile.setFilePath(indexFileName);
						// buyerIdIndexList.put(indexFileName,
						// buyerIdHashTable);
						/*smallFile.setOrderIdIndex(orderIdHashTable
								.writeAllBuckets());
						smallFile.setOrderBuyerIdIndex(orderBuyerIdHashTable
								.writeAllBuckets());
						smallFile.setOrderGoodIdIndex(orderGoodIdHashTable
								.writeAllBuckets());*/
						smallFile.setOrderIdIndex(0);
						smallFile.setOrderBuyerIdIndex(0);
						smallFile.setOrderGoodIdIndex(0);

						orderIdIndexList.put(indexFileName, orderIdHashTable);
						orderBuyerIdIndexList.put(indexFileName,
								orderBuyerIdHashTable);
						orderGoodIdIndexList.put(indexFileName,
								orderGoodIdHashTable);
						BucketCachePool.getInstance().removeAllBucket();
						orderFileList.add(smallFile);
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
