package com.alibaba.middleware.disruptor;

import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.handlefile.IndexItem;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.tools.BytesKey;
import com.alibaba.middleware.tools.RecordsUtils;
import com.lmax.disruptor.EventHandler;

public class OrderEventHandler implements EventHandler<RecordsEvent>{

	private int count = 0;								// 用于判断是否该建下一个索引了
	private String indexFileName = null;
	private int fileIndex = 0;
	private DiskHashTable idHashTable = null;
	//HashSet<String> tempAttrList = new HashSet<String>();
	private IndexType indexType = null;
	private OrderSystemImpl system = null;
	private CountDownLatch countDownLatch = null;
	private String orderFileNamePrex = null;
	private int indexFileNumber = 0;
	
	public OrderEventHandler( OrderSystemImpl system, IndexType indexType, CountDownLatch countDownLatch) {
		this.system = system;
		this.indexType = indexType;
		this.countDownLatch = countDownLatch;
		orderFileNamePrex = RaceConfig.storeFolders[0] + RaceConfig.orderFileNamePrex;
	}
	
	@Override
	public void onEvent(RecordsEvent event, long sequence, boolean endOfBatch)
			throws Exception {
		// TODO Auto-generated method stub
		if (event.getRecordsData() != null) {

			if (indexFileName == null) {
				// 第一次建立索引文件
				indexFileName = orderFileNamePrex + indexFileNumber;
				// 添加到文件mapping里
				fileIndex = system.orderIndexMapping.addDataFileName(indexFileName);
				switch(indexType) {
				case OrderId:
					String diskFileName = RaceConfig.storeFolders[0]
							+ indexFileName.replace("/", "_").replace("//", "_");
					System.out.println("create order index:" + diskFileName);
					idHashTable = new DiskHashTable<Long>(system,
							diskFileName
							+ RaceConfig.orderIndexFileSuffix,DirectMemoryType.NoWrite);
					break;
				case OrderBuyerId:
					idHashTable = system.buyerIdIndexList.get(0);
					/*String orderBuyerDiskFileName = RaceConfig.storeFolders[(threadIndex + 1) % 3]
							+ indexFileName.replace("/", "_").replace("//", "_");
					idHashTable = new DiskHashTable<Integer, List<byte[]>>(system,
							orderBuyerDiskFileName
							+ RaceConfig.orderBuyerIdIndexFileSuffix, List.class
							,DirectMemoryType.BuyerIdSegment);*/
					break;
				case OrderGoodId:
					idHashTable = system.goodIdIndexList.get(0);
					/*String orderGoodDiskFileName = RaceConfig.storeFolders[(threadIndex + 2) % 3]
							+ indexFileName.replace("/", "_").replace("//", "_");
					idHashTable = new DiskHashTable<Integer, List<byte[]>>(system,
							orderGoodDiskFileName
							+ RaceConfig.orderGoodIdIndexFileSuffix, List.class,
							DirectMemoryType.GoodIdSegment);*/
					break;
				}

			}
			
			if( count == RaceConfig.maxIndexFileCapacity) {
				// 说明要开始建下一个索引了
				switch(indexType) {
				case OrderId:
					// 保存当前goodId的索引 并写入索引List
					idHashTable.writeAllBuckets();
					system.orderIdIndexList.put(fileIndex, idHashTable);
					indexFileNumber ++;
					indexFileName = orderFileNamePrex + indexFileNumber;
					
					String diskFileName = RaceConfig.storeFolders[0]
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
				count = 0;				// count置0
				fileIndex = system.orderIndexMapping.addDataFileName(indexFileName);
			}
			// 计数器加1
			count ++;

			switch(indexType) {
			case OrderId:
				//tempAttrList.addAll(rowData.keySet());
				long orderId = Long.parseLong(RecordsUtils.getValueFromLine(
						event.getRecordsData(),RaceConfig.orderId));
				// 将order表的数据放入缓冲区
				//rowCache.putInCache(new BytesKey(record.getOffset()), record.getRecordsData(), TableName.OrderTable);
				idHashTable.put(orderId, event.getOffset());
				break;
			case OrderBuyerId:
				// 这里要从小表索引里拿出数据来修改
				String buyerId = RecordsUtils.getValueFromLine(
						event.getRecordsData(),RaceConfig.buyerId);
				//for( DiskHashTable modifyTable : buyerIdIndexList.values()) {
				idHashTable.putOffset( new BytesKey(buyerId.getBytes()),event.getOffset());
				//}
				//idHashTable.put(buyerIdHashCode, record.getOffset());

				break;
			case OrderGoodId:
				String goodId = RecordsUtils.getValueFromLine(
						event.getRecordsData(),RaceConfig.goodId);
				//for( DiskHashTable modifyTable : goodIdIndexList.values()) {
				idHashTable.putOffset( new BytesKey(goodId.getBytes()),event.getOffset());
				//}
				//idHashTable.put(goodIdHashCode, record.getOffset());
				break;
			}
			
		} else {
			// recordsData等于null说明消费结束了
			// 保存当前goodId的索引 并写入索引List
			switch(indexType) {
			case OrderId:
				idHashTable.writeAllBuckets();
				system.orderIdIndexList.put(fileIndex, idHashTable);
				break;
			case OrderBuyerId:
				// 这里可以选择把两个小表索引写到直接内存里面去
				// 把direct memory剩余的内容dump到文件里去
				//for( DiskHashTable modifyTable : system.buyerIdIndexList.values()) {
				//	modifyTable.dumpDirectMemory();
				//}
				//idHashTable.writeAllBuckets();
				//orderBuyerIdIndexList.put(fileIndex, idHashTable);
				break;
			case OrderGoodId:
				//for( DiskHashTable modifyTable : system.goodIdIndexList.values()) {
				//	modifyTable.dumpDirectMemory();
				//}
//				idHashTable.writeAllBuckets();
//				orderGoodIdIndexList.put(fileIndex,idHashTable);
				break;
			}
			BucketCachePool.getInstance().removeAllBucket();
			system.waitForConstruct.getAndIncrement();
			countDownLatch.countDown();

		}
		// 处理完
		event = null;
	}

}
