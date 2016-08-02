package com.alibaba.middleware.threads;

import com.alibaba.middleware.cache.ConcurrentCache;
import com.alibaba.middleware.cache.DirectMemoryCache;
import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.conf.RaceConfig.IdIndexType;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.handlefile.FileIndexWithOffset;
import com.alibaba.middleware.index.ByteDirectMemory;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.ResultImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.ByteUtils;
import com.alibaba.middleware.tools.BytesKey;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * @author Jelly
 */
public class QueryOrderByBuyerThread  {
    private long startTime;
    private long endTime;
    private String buyerid;
    private OrderSystemImpl system;
    private ByteDirectMemory directMemory = null;

    public QueryOrderByBuyerThread(OrderSystemImpl system, long startTime, long endTime, String buyerid) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.buyerid = buyerid;
        this.system = system;
        directMemory = ByteDirectMemory.getInstance();
    }
    
    /**
     * 将符合条件的row放入结果集中
     * @param row
     * @param results
     */
    public void handleOffsetList( List<byte[]> offsets, TreeMap<Long, List<Result>> results) {
		
		try {
			for( byte[] encodedOffset : offsets) {
				// 先在缓冲区里找
				// 这里要将offset解析成文件下标+offset的形式
				ByteBuffer buffer = ByteBuffer.wrap(encodedOffset);
				// 从byte解析出int			
				int fileIndex = ByteUtils.getMagicIntFromByte(buffer.get());
				long offset = ByteUtils.getLongOffset(buffer.getInt());
				//Row row = rowCache.getFromCache(new BytesKey(encodedOffset), TableName.OrderTable);
				//if(row != null) {
				//	row = row.getKV(RaceConfig.buyerId).valueAsString().equals(buyerid) ?
				//			row : RecordsUtils.createKVMapFromLine(RecordsUtils.getStringFromFile(
				//					system.orderHandlersList.get(fileIndex), offset, TableName.OrderTable));
				//}
				//else {
				// 在硬盘里找数据
				String diskData = RecordsUtils.getStringFromFile(
						system.orderHandlersList.get(fileIndex), offset, TableName.OrderTable);
				//rowCache.putInCache(new BytesKey(encodedOffset), diskData, TableName.OrderTable);
				// 放入缓冲区
				
				String goodid = RecordsUtils.getValueFromLine(diskData, RaceConfig.goodId);
				long createTime = Long.parseLong(RecordsUtils.getValueFromLine(
						diskData, RaceConfig.createTime));
				if(createTime >= startTime && createTime < endTime) {
					// 判断时间范围符合要求
					// 加上其他表的所有数据
					long orderid = Long.parseLong(RecordsUtils.getValueFromLine(
							diskData, RaceConfig.orderId));
					StringBuilder resultBuilder = new StringBuilder();
					resultBuilder.append(diskData).append("\t");
					resultBuilder.append(system.getRowStringById(
							TableName.BuyerTable, buyerid)).append("\t");
					
					resultBuilder.append(system.getRowStringById(TableName.GoodTable, goodid));
					List<Result> smallResults = results.get(createTime);
					if(smallResults == null) {
						smallResults = new ArrayList<Result>();
						results.put(createTime, smallResults);
					}
					smallResults.add(new ResultImpl(orderid, 
							RecordsUtils.createKVMapFromLine(resultBuilder.toString())));
				}
			}
			
		} catch (TypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

    /**
     * 第二个查询
	 * 查询某位买家createtime字段从[startTime, endTime) 时间范围内发生的所有订单的所有信息
	 * 
	 * @param startTime
	 *            订单创建时间的下界
	 * @param endTime
	 *            订单创建时间的上界
	 * @param buyerid
	 *            买家Id
	 * @return 符合条件的订单集合，按照createtime大到小排列
	 */
    public Iterator<Result> getResult() {
    	// 根据买家ID在索引里找到结果 再判断结果是否介于startTime和endTime之间 结果集合按照createTime插入排序
		TreeMap<Long, List<Result>> results = new TreeMap<Long, List<Result>>(
				Collections.reverseOrder());
		BytesKey surrId = new BytesKey(buyerid.getBytes());
		//boolean isCached = false;

		// 先在缓存里找有没有对应的orderId列表
		/*List<byte[]> offsetList = rowCache.getFromIdCache(surrId, IdIndexType.BuyerIdToOrderOffsets);
		if( offsetList != null && !offsetList.isEmpty()) {
			// 说明缓冲区里找到了
			isCached = true;
			handleOffsetList(offsetList, results);
		}
		if( !isCached) {*/
			// 没找到则在索引里找offsetlist
		List<byte[]> offsetList = new ArrayList<byte[]>();
		for( int filePathIndex : system.buyerIndexMapping.getAllFileIndexs()) {
			DiskHashTable<BytesKey> hashTable = 
					system.buyerIdIndexList.get(filePathIndex);
			// 一次性解析所有offset
			byte[] offsets = hashTable.get(surrId);
			if( offsets != null) {
				// 解析出offset列表
				
				ByteBuffer tempBuffer = ByteBuffer.wrap(offsets);
				// 跳过标识位以及本身数据的offset
				tempBuffer.position(RaceConfig.byte_size + RaceConfig.compressed_min_bytes_length);
				// 得到所有的offset
				List<byte[]> byteAndInts = ByteUtils.splitByteBuffer(tempBuffer);
				for( byte[] byteAndOffset : byteAndInts) {
					// 从orderid列表中取出相应的数据
					// 从byte解析出int
					int memoryIndex = byteAndOffset[0];
					// 跳过第一个字节
					int pos = ByteUtils.byteArrayToLeInt(Arrays.copyOfRange(byteAndOffset, 
							1, byteAndOffset.length));
					
					if( memoryIndex >= 0) {
						// 说明是在直接内存里
						offsetList.addAll(directMemory.getOrderIdListsFromBytes(memoryIndex, pos));
					}
					else {
						// 说明自己就是数据的offset
						offsetList.add(byteAndOffset);
					}
					
				}
			}
		}
		/*for (int filePathIndex : system.orderIndexMapping.getAllFileIndexs()) {
			DiskHashTable<byte[], byte[]> hashTable = 
					system.orderBuyerIdIndexList.get(filePathIndex);
			List<byte[]> offSetresults = hashTable.get(surrId);
			if (offSetresults.size() != 0) {
				// find the records offset
				offsets.addAll(offSetresults);
				
			}
		}*/
		// 将offsetlist放入缓冲区
		//rowCache.putInIdCache(surrId, offsets, IdIndexType.BuyerIdToOrderOffsets);
		handleOffsetList(offsetList, results);
		//}
		
		List<Result> returnResults = new ArrayList<Result>();
		for(List<Result> r : results.values() ) {
			returnResults.addAll(r);
		}
		return returnResults.iterator();
    }
	
	
}
