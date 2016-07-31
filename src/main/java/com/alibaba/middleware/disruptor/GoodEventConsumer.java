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

public class GoodEventConsumer implements EventHandler<IndexItem>{

	int fileIndex = 0;
	String indexFileName = null;
	DiskHashTable<BytesKey> goodIdHashTable = null;
	boolean isEnd = false;
	HashSet<String> tempAttrList = new HashSet<String>();
	//long surrKey = 1;
	
	@Override
	public void onEvent(IndexItem event, long sequence, boolean endOfBatch)
			throws Exception {
		// TODO Auto-generated method stub

		if( event != null ) {
			if( event.getRecordsData() == null) {
				isEnd = true;
				continue;
			}

			if( !event.getIndexFileName().equals(indexFileName)) {
				if( indexFileName == null) {
					// 第一次建立索引文件
					indexFileName = event.getIndexFileName();
					String diskFileName = RaceConfig.storeFolders[(threadIndex + 2) % 3]
							+ indexFileName.replace("/", "_").replace("//", "_");
					System.out.println("create good index:" + diskFileName);
					fileIndex = goodIndexMapping.addDataFileName(indexFileName);
					goodIdHashTable = new DiskHashTable<BytesKey>(system,
							diskFileName + RaceConfig.goodIndexFileSuffix,
							DirectMemoryType.GoodIdSegment);

				}
				else {
					// 保存当前goodId的索引  并写入索引List
					//goodIdHashTable.writeAllBuckets();
					//smallFile.setGoodIdIndex(0);
					goodIdIndexList.put(fileIndex, goodIdHashTable);	
					indexFileName = event.getIndexFileName();
					// 将三张表的索引写到不同的disk里去
					String diskFileName = RaceConfig.storeFolders[(threadIndex + 2) % 3]
							+ indexFileName.replace("/", "_").replace("//", "_");
					System.out.println("create good index:" + diskFileName);
					fileIndex = goodIndexMapping.addDataFileName(indexFileName);
					goodIdHashTable = new DiskHashTable<BytesKey>(system,
							diskFileName + RaceConfig.goodIndexFileSuffix,
							DirectMemoryType.GoodIdSegment);


				}
			}
			//Row rowData = Row.createKVMapFromLine(record.getRecordsData());	
			//tempAttrList.addAll(rowData.keySet());
			//String goodid = rowData.getKV(RaceConfig.goodId).valueAsString();
			//Integer goodIdHashCode = goodid.hashCode();
			// 放入缓冲区
			//rowCache.putInCache(goodIdHashCode, record.getRecordsData(), TableName.GoodTable);
			goodIdHashTable.put(new BytesKey(RecordsUtils.getValueFromLineWithKeyList(
					event.getRecordsData(),RaceConfig.goodId, tempAttrList).getBytes()), 
					event.getOffset());
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
			//goodIdHashTable.writeAllBuckets();

			//smallFile.setGoodIdIndex(0);
			BucketCachePool.getInstance().removeAllBucket();
			goodIdIndexList.put(fileIndex, goodIdHashTable);
			latch.countDown();
			break;
		}
	}

}
