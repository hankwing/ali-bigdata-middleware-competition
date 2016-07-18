package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
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
	ConcurrentHashMap<String, DiskHashTable<Long, List<Long>>> orderBuyerIdIndexList = null;
	ConcurrentHashMap<String, DiskHashTable<Long, List<Long>>> orderGoodIdIndexList = null;
	ConcurrentHashMap<String, List<DiskHashTable<Long, List<Long>>>> orderCountableIndexList = null;
	DiskHashTable<String, Long> buyerIdSurrKeyIndex = null;
	DiskHashTable<String, Long> goodIdSurrKeyIndex = null;
	List<FilePathWithIndex> orderFileList = null;
	List<String> orderAttrList = null;
	int threadIndex = 0;

	public OrderHandler(
			ConcurrentHashMap<String, DiskHashTable<Long, Long>> orderIdIndexList,
			ConcurrentHashMap<String, DiskHashTable<Long, List<Long>>> orderBuyerIdIndexList,
			ConcurrentHashMap<String, DiskHashTable<Long, List<Long>>> orderGoodIdIndexList,
			ConcurrentHashMap<String, List<DiskHashTable<Long, List<Long>>>> orderCountableIndexList,
			List<FilePathWithIndex> orderFileList, List<String> orderAttrList,
			DiskHashTable<String, Long> buyerIdSurrKeyIndex,
			DiskHashTable<String, Long> goodIdSurrKeyIndex, int thread) {

		this.orderIdIndexList = orderIdIndexList;
		this.orderBuyerIdIndexList = orderBuyerIdIndexList;
		this.orderGoodIdIndexList = orderGoodIdIndexList;
		this.orderCountableIndexList = orderCountableIndexList;
		this.orderFileList = orderFileList;
		this.orderAttrList = orderAttrList;
		this.buyerIdSurrKeyIndex = buyerIdSurrKeyIndex;
		this.goodIdSurrKeyIndex = goodIdSurrKeyIndex;
		threadIndex = thread;
		indexQueue = new LinkedBlockingQueue<IndexItem>();
		orderfile = new WriteFile(indexQueue,
				RaceConfig.storeFolders[threadIndex],
				RaceConfig.orderFileNamePrex,
				(int) RaceConfig.smallFileCapacity);
	}

	public void HandleOrderFiles(List<String> files) {
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
					orderfile.writeLine(record, IndexType.OrderTable);
					record = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		orderfile.closeFile();

		// set end signal
		orderfile.writeLine("end", IndexType.OrderTable);
		orderfile.closeFile();

		orderfile.closeFile();
		System.out.println("end order handling!");
	}

	// order表的建索引线程 需要建的索引包括：orderid, buyerid, goodid, countable 字段的索引
	public class GoodIndexConstructor implements Runnable {

		String indexFileName = "";
		DiskHashTable<Long, Long> orderIdHashTable = null;
		DiskHashTable<Long, List<Long>> orderBuyerIdHashTable = null;
		DiskHashTable<Long, List<Long>> orderGoodIdHashTable = null;
		boolean isEnd = false;

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
						}

						if (!record.getFileName().equals(indexFileName)) {
							if (indexFileName == null) {
								// 第一次建立索引文件
								indexFileName = record.getFileName();
								orderIdHashTable = new DiskHashTable<Long, Long>(
										indexFileName
												+ RaceConfig.orderIndexFileSuffix,
										indexFileName, Long.class);
								orderBuyerIdHashTable = new DiskHashTable<Long, List<Long>>(
										indexFileName
												+ RaceConfig.orderIndexFileSuffix,
										indexFileName, Long.class);
								orderGoodIdHashTable = new DiskHashTable<Long, List<Long>>(
										indexFileName
												+ RaceConfig.orderIndexFileSuffix,
										indexFileName, Long.class);

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
								orderBuyerIdHashTable = new DiskHashTable<Long, List<Long>>(
										indexFileName
												+ RaceConfig.orderIndexFileSuffix,
										indexFileName, Long.class);
								orderGoodIdHashTable = new DiskHashTable<Long, List<Long>>(
										indexFileName
												+ RaceConfig.orderIndexFileSuffix,
										indexFileName, Long.class);

							}
						}

						Row recordRow = Row
								.createKVMapFromLine(record.recordsData);
						long orderid = recordRow.get(RaceConfig.orderId)
								.valueAsLong();

						// 获取代理键
						long agentBuyerId = buyerIdSurrKeyIndex
								.get(recordRow.get(RaceConfig.buyerId)
										.valueAsLong()).get(0);
						long agentGoodId = goodIdSurrKeyIndex.get(
								recordRow.get(RaceConfig.goodId).valueAsLong())
								.get(0);

						// 建立三个索引
						orderIdHashTable.put(orderid, record.getOffset());
						orderBuyerIdHashTable.put(agentBuyerId,
								record.getOffset());
						orderGoodIdHashTable.put(agentGoodId,
								record.getOffset());

					} else if (isEnd) {
						// 保存当前goodId的索引 并写入索引List
						FilePathWithIndex smallFile = new FilePathWithIndex();
						smallFile.setFilePath(indexFileName);
						// buyerIdIndexList.put(indexFileName,
						// buyerIdHashTable);
						smallFile.setOrderIdIndex(orderIdHashTable
								.writeAllBuckets());
						smallFile.setOrderBuyerIdIndex(orderBuyerIdHashTable
								.writeAllBuckets());
						smallFile.setOrderGoodIdIndex(orderGoodIdHashTable
								.writeAllBuckets());

						orderIdIndexList.put(indexFileName, orderIdHashTable);
						orderBuyerIdIndexList.put(indexFileName,
								orderBuyerIdHashTable);
						orderGoodIdIndexList.put(indexFileName,
								orderGoodIdHashTable);
						orderFileList.add(smallFile);

						break;

					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
}
