package com.alibaba.middleware.threads;

import com.alibaba.middleware.cache.ConcurrentCache;
import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdIndexType;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.handlefile.FileIndexWithOffset;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.ResultImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.BytesKey;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * @author Jelly
 */
public class QueryOrderByBuyerThread extends QueryThread<Iterator<Result>> {
    private long startTime;
    private long endTime;
    private String buyerid;
    private OrderSystemImpl system;
    private ConcurrentCache rowCache = null;

    public QueryOrderByBuyerThread(OrderSystemImpl system, long startTime, long endTime, String buyerid) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.buyerid = buyerid;
        this.system = system;
        rowCache = ConcurrentCache.getInstance();
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
				int fileIndex = buffer.getInt();
				long offset = buffer.getLong();
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
				
				
				if( RecordsUtils.getValueFromLine(diskData, RaceConfig.buyerId).equals(buyerid)) {
					// 确认一下 避免某些key的hashcode相同
					//rowCache.putInCache(new BytesKey(encodedOffset), diskData, TableName.OrderTable);
					// 放入缓冲区
					StringBuilder resultBuilder = new StringBuilder();
					resultBuilder.append(diskData).append("\t");
					String goodid = RecordsUtils.getValueFromLine(diskData, RaceConfig.goodId);
					long createTime = Long.parseLong(RecordsUtils.getValueFromLine(
							diskData, RaceConfig.createTime));
					long orderid = Long.parseLong(RecordsUtils.getValueFromLine(
							diskData, RaceConfig.orderId));
					if(createTime >= startTime && createTime < endTime) {
						// 判断时间范围符合要求
						// 加上其他表的所有数据
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
	@Override
    public Iterator<Result> call() throws Exception {
    	// 根据买家ID在索引里找到结果 再判断结果是否介于startTime和endTime之间 结果集合按照createTime插入排序
		TreeMap<Long, List<Result>> results = new TreeMap<Long, List<Result>>(
				Collections.reverseOrder());
		Integer surrId = buyerid.hashCode();
		boolean isCached = false;
		if( surrId == 0) {
			//不存在该买家
			return null;
		}
		else {
			// 先在缓存里找有没有对应的orderId列表
			List<byte[]> offsetList = rowCache.getFromIdCache(surrId, IdIndexType.BuyerIdToOrderOffsets);
			if( offsetList != null && !offsetList.isEmpty()) {
				ByteBuffer buffer = ByteBuffer.wrap(offsetList.get(0));
				int fileIndex = buffer.getInt();
				long offset = buffer.getLong();
				String diskData = RecordsUtils.getStringFromFile(
						system.orderHandlersList.get(fileIndex), offset,
						TableName.OrderTable);
				if (RecordsUtils.getValueFromLine(diskData, RaceConfig.buyerId)
						.equals(buyerid)) {
					// 说明缓冲区里找到了
					isCached = true;
					handleOffsetList(offsetList, results);
				}
			}
			if( !isCached) {
				// 没找到则在索引里找offsetlist
				List<byte[]> offsets = new ArrayList<byte[]>();
				for (int filePathIndex : system.orderIndexMapping.getAllFileIndexs()) {
					DiskHashTable<Integer, List<byte[]>> hashTable = 
							system.orderBuyerIdIndexList.get(filePathIndex);
					List<byte[]> offSetresults = hashTable.get(surrId);
					if (offSetresults.size() != 0) {
						// find the records offset
						offsets.addAll(offSetresults);
						
					}
				}
				// 将offsetlist放入缓冲区
				rowCache.putInIdCache(surrId, offsets, IdIndexType.BuyerIdToOrderOffsets);
				handleOffsetList(offsets, results);
			}
			
		}
		
		List<Result> returnResults = new ArrayList<Result>();
		for(List<Result> r : results.values() ) {
			returnResults.addAll(r);
		}
		return returnResults.iterator();
    }
	
	
}
