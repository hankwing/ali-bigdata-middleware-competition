package com.alibaba.middleware.threads;

import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdIndexType;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.ResultImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

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
    private SimpleCache rowCache = null;

    public QueryOrderByBuyerThread(OrderSystemImpl system, long startTime, long endTime, String buyerid) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.buyerid = buyerid;
        this.system = system;
        rowCache = SimpleCache.getInstance();
    }
    
    /**
     * 将符合条件的row放入结果集中
     * @param row
     * @param results
     */
    public void addResultRow( Row row, TreeMap<Long, List<Result>> results) {
		
		try {
			long createTime = row.getKV(RaceConfig.createTime).valueAsLong();
			long orderid = row.getKV(RaceConfig.orderId).valueAsLong();
			if( row.getKV(RaceConfig.buyerId).valueAsString().equals(buyerid) &&
					createTime >= startTime && createTime < endTime) {
				// 判断买家id是否符合  并且时间范围符合要求
				
				row.putAll(system.getRowById(TableName.BuyerTable,
						row.get(RaceConfig.buyerId).valueAsString()));			
				// need query goodTable
				row.putAll(system.getRowById(TableName.GoodTable,
						row.get(RaceConfig.goodId).valueAsString()));
				List<Result> smallResults = results.get(createTime);
				if(smallResults == null) {
					smallResults = new ArrayList<Result>();
					results.put(createTime, smallResults);
				}
				smallResults.add(new ResultImpl(orderid, row));
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
		if( surrId == 0) {
			//不存在该买家
			return null;
		}
		else {
			// 先在缓存里找有没有对应的orderId列表
			List<Long> orderIds = rowCache.getFormIdCache(surrId, IdIndexType.BuyerIdToOrderId);
			if( orderIds != null) {
				// 在缓冲区里找到了buyerid对应的orderid列表
				for( long orderId : orderIds) {
					Row row = system.getRowById(TableName.OrderTable, orderId);
					if( row != null) {
						addResultRow(row, results);
					}
				}
			}
			else {
				for (FilePathWithIndex filePath : system.orderFileList) {
					DiskHashTable<Integer, List<Long>> hashTable = system.orderBuyerIdIndexList.get(filePath
							.getFilePath());
					List<Long> offSetresults = hashTable.get(surrId);
					if (offSetresults.size() != 0) {
						// find the records offset
						// 找到后，按照降序插入TreeMap中
						for( Long offset: offSetresults) {
							Row row = Row.createKVMapFromLine(RecordsUtils.getStringFromFile(
									filePath, offset, TableName.OrderTable));
							addResultRow(row, results);
							
						}
					}

				}
			}
			
		}
		
		List<Result> returnResults = new ArrayList<Result>();
		for(List<Result> r : results.values() ) {
			returnResults.addAll(r);
		}
		return returnResults.iterator();
    }
	
	
}
