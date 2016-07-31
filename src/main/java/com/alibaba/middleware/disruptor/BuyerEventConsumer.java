package com.alibaba.middleware.disruptor;

import java.util.HashSet;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.handlefile.IndexItem;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.tools.BytesKey;
import com.alibaba.middleware.tools.RecordsUtils;
import com.lmax.disruptor.EventHandler;

public class BuyerEventConsumer implements EventHandler<IndexItem> {

	// buyer表的建索引线程 需要建的索引包括：代理键索引和buyerId的索引

	String indexFileName = null;
	DiskHashTable<BytesKey> buyerIdHashTable = null;
	boolean isEnd = false;
	HashSet<String> tempAttrList = new HashSet<String>();
	int fileIndex = 0;

	// long surrKey = 1;

	public BuyerEventConsumer() {

	}

	@Override
	public void onEvent(IndexItem event, long sequence, boolean endOfBatch)
			throws Exception {
		// TODO Auto-generated method stub
		if (event != null) {
			if (event.getRecordsData() == null) {
				isEnd = true;
				continue;
			}

			if (!event.getIndexFileName().equals(indexFileName)) {
				if (indexFileName == null) {
					// 第一次建立索引文件
					indexFileName = event.getIndexFileName();
					String diskFileName = RaceConfig.storeFolders[(threadIndex + 1) % 3]
							+ indexFileName.replace("/", "_").replace(
									"//", "_");
					fileIndex = buyerIndexMapping
							.addDataFileName(indexFileName);
					System.out.println("create buyer index:"
							+ diskFileName);
					buyerIdHashTable = new DiskHashTable<BytesKey>(
							system, diskFileName
									+ RaceConfig.buyerIndexFileSuffix,
							DirectMemoryType.BuyerIdSegment);

				} else {
					// 保存当前buyerId的索引 并写入索引List
					// buyerIdHashTable.writeAllBuckets();
					// smallFile.setBuyerIdIndex(0);
					buyerIdIndexList.put(fileIndex, buyerIdHashTable);
					indexFileName = event.getIndexFileName();
					String diskFileName = RaceConfig.storeFolders[(threadIndex + 1) % 3]
							+ indexFileName.replace("/", "_").replace(
									"//", "_");
					fileIndex = buyerIndexMapping
							.addDataFileName(indexFileName);
					System.out.println("create buyer index:"
							+ diskFileName);
					buyerIdHashTable = new DiskHashTable<BytesKey>(
							system, diskFileName
									+ RaceConfig.buyerIndexFileSuffix,
							DirectMemoryType.BuyerIdSegment);

				}
			}

			// tempAttrList.addAll(rowData.keySet()); // 添加属性
			// String buyerid =
			// rowData.getKV(RaceConfig.buyerId).valueAsString();
			// Integer buyerIdHashCode = buyerid.hashCode();
			// 放入缓冲区中
			// rowCache.putInCache(buyerIdHashCode,
			// record.getRecordsData(), TableName.BuyerTable);
			// buyerIdSurrKeyIndex.put(buyerid, surrKey); // 建立代理键索引
			buyerIdHashTable.put(
					new BytesKey(RecordsUtils
							.getValueFromLineWithKeyList(
									event.getRecordsData(),
									RaceConfig.buyerId, tempAttrList)
							.getBytes()), event.getOffset());
		} else if (isEnd) {
			// 说明队列为空
			// 将代理键索引写出去 并保存相应数据 将buyerid索引写出去 并保存相应数据
			// buyerIdSurrKeyFile.setFilePath(RaceConfig.buyerSurrFileName);
			// buyerIdSurrKeyFile.setSurrogateIndex(buyerIdSurrKeyIndex.writeAllBuckets());
			synchronized (buyerAttrList) {
				buyerAttrList.addAll(tempAttrList);
			}
			BucketCachePool.getInstance().removeAllBucket();
			// 先不把小表索引写出去了 留在内存里 等order索引建完后再写到直接内存里
			// buyerIdHashTable.writeAllBuckets();
			buyerIdIndexList.put(fileIndex, buyerIdHashTable);
			latch.countDown();
			break;
		}
	}
}
