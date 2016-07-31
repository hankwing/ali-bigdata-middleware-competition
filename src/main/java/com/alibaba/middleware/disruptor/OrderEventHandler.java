package com.alibaba.middleware.disruptor;

import java.util.concurrent.ArrayBlockingQueue;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.handlefile.IndexItem;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.tools.BytesKey;
import com.alibaba.middleware.tools.RecordsUtils;
import com.lmax.disruptor.EventHandler;

public class OrderEventHandler implements EventHandler<IndexItem>{


	String indexFileName = null;
	int fileIndex = 0;
	DiskHashTable idHashTable = null;
	boolean isEnd = false;
	//HashSet<String> tempAttrList = new HashSet<String>();
	IndexType indexType = null;
	ArrayBlockingQueue<IndexItem> indexQueue;
	
	public OrderEventHandler( IndexType indexType, ArrayBlockingQueue<IndexItem> indexQueue) {
		this.indexType = indexType;
		this.indexQueue = indexQueue;
	}
	
	@Override
	public void onEvent(IndexItem event, long sequence, boolean endOfBatch)
			throws Exception {
		// TODO Auto-generated method stub
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
								+ indexFileName.replace("/", "_").replace("//", "_");
						System.out.println("create order index:" + diskFileName);
						idHashTable = new DiskHashTable<Long>(system,
								diskFileName
								+ RaceConfig.orderIndexFileSuffix,DirectMemoryType.NoWrite);
						break;
					case OrderBuyerId:
						idHashTable = buyerIdIndexList.get(0);
						/*String orderBuyerDiskFileName = RaceConfig.storeFolders[(threadIndex + 1) % 3]
								+ indexFileName.replace("/", "_").replace("//", "_");
						idHashTable = new DiskHashTable<Integer, List<byte[]>>(system,
								orderBuyerDiskFileName
								+ RaceConfig.orderBuyerIdIndexFileSuffix, List.class
								,DirectMemoryType.BuyerIdSegment);*/
						break;
					case OrderGoodId:
						idHashTable = goodIdIndexList.get(0);
						/*String orderGoodDiskFileName = RaceConfig.storeFolders[(threadIndex + 2) % 3]
								+ indexFileName.replace("/", "_").replace("//", "_");
						idHashTable = new DiskHashTable<Integer, List<byte[]>>(system,
								orderGoodDiskFileName
								+ RaceConfig.orderGoodIdIndexFileSuffix, List.class,
								DirectMemoryType.GoodIdSegment);*/
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
								+ indexFileName.replace("/", "_").replace("//", "_");
						System.out.println("create order index:" + diskFileName);
						idHashTable = new DiskHashTable<Long>(system,
								diskFileName
								+ RaceConfig.orderIndexFileSuffix,
								DirectMemoryType.NoWrite);
						break;
					case OrderBuyerId:
						/*orderBuyerIdIndexList.put(fileIndex, idHashTable);
						idHashTable.writeAllBuckets();
						indexFileName = record.getIndexFileName();
						String orderBuyerDiskFileName = RaceConfig.storeFolders[(threadIndex + 1) % 3]
								+ indexFileName.replace("/", "_").replace("//", "_");
						idHashTable = new DiskHashTable<Integer, List<byte[]>>(
								orderBuyerDiskFileName
								+ RaceConfig.orderBuyerIdIndexFileSuffix, List.class,
								DirectMemoryType.BuyerIdSegment);*/
						break;
					case OrderGoodId:
						/*orderGoodIdIndexList.put(fileIndex, idHashTable);
						idHashTable.writeAllBuckets();
						indexFileName = record.getIndexFileName();
						String orderGoodDiskFileName = RaceConfig.storeFolders[(threadIndex + 2) % 3]
								+ indexFileName.replace("/", "_").replace("//", "_");
						idHashTable = new DiskHashTable<Integer, List<byte[]>>(
								orderGoodDiskFileName
								+ RaceConfig.orderGoodIdIndexFileSuffix, List.class,
								DirectMemoryType.GoodIdSegment);*/
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
				// 这里要从小表索引里拿出数据来修改
				String buyerId = RecordsUtils.getValueFromLine(
						record.getRecordsData(),RaceConfig.buyerId);
				//for( DiskHashTable modifyTable : buyerIdIndexList.values()) {
				idHashTable.putOffset( new BytesKey(buyerId.getBytes()),record.getOffset());
				//}
				//idHashTable.put(buyerIdHashCode, record.getOffset());

				break;
			case OrderGoodId:
				String goodId = RecordsUtils.getValueFromLine(
						record.getRecordsData(),RaceConfig.goodId);
				//for( DiskHashTable modifyTable : goodIdIndexList.values()) {
				idHashTable.putOffset( new BytesKey(goodId.getBytes()),record.getOffset());
				//}
				//idHashTable.put(goodIdHashCode, record.getOffset());
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
				// 这里可以选择把两个小表索引写到直接内存里面去
				// 把direct memory剩余的内容dump到文件里去
				for( DiskHashTable modifyTable : buyerIdIndexList.values()) {
					modifyTable.dumpDirectMemory();
				}
				//idHashTable.writeAllBuckets();
				//orderBuyerIdIndexList.put(fileIndex, idHashTable);
				break;
			case OrderGoodId:
				for( DiskHashTable modifyTable : goodIdIndexList.values()) {
					modifyTable.dumpDirectMemory();
				}
//				idHashTable.writeAllBuckets();
//				orderGoodIdIndexList.put(fileIndex,idHashTable);
				break;
			}
			BucketCachePool.getInstance().removeAllBucket();
			system.waitForConstruct.getAndIncrement();
			countDownLatch.countDown();
			break;

		}
	}

}
