package com.alibaba.middleware.threads;

import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdIndexType;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.ResultImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * @author Jelly
 */
public class QueryOrdersBySalerThread extends QueryThread<Iterator<Result>> {
    private String salerid;
    private String goodid;
    private Collection<String> keys;
    private OrderSystemImpl system = null;
    private SimpleCache rowCache = null;

    public QueryOrdersBySalerThread(OrderSystemImpl system,
    		String salerid, String goodid, Collection<String> keys) {
    	rowCache = SimpleCache.getInstance();
    	this.system = system;
        this.salerid = salerid;
        this.goodid = goodid;
        this.keys = keys;
    }
    
    /**
     * 判断row是否符合要求 符合则加入结果集
     * @param row
     * @param results
     */
    public void handleOffsets( List<Long> offsets, TreeMap<Long, Result> results, List<String> buyerKeys,
    		List<String> goodKeys) {
    	
    	for( Long offset: offsets) {
			// 放入缓冲区
    		// 这里要将offset解析成文件下标+offset的形式才能用
			Row row = system.rowCache.getFromCache(offset, TableName.OrderTable);
			if(row != null) {
				row = row.getKV(RaceConfig.buyerId).valueAsString().equals(buyerid) ?
						row : Row.createKVMapFromLine(RecordsUtils.getStringFromFile(
								system.fileHandlersList.get(key), offset, TableName.OrderTable));
			}
			else {
				// 在硬盘里找数据
				String diskData = RecordsUtils.getStringFromFile(
						system.fileHandlersList.get(key), offset, TableName.OrderTable);
				row = Row.createKVMapFromLine(diskData);
				rowCache.putInCache(offset, diskData, TableName.OrderTable);
				// 放入缓冲区
			}

			try {
				long orderId = row.getKV(RaceConfig.orderId).valueAsLong();
				if( keys == null) {
					row.putAll(system.getRowById(TableName.BuyerTable, 
							row.get(RaceConfig.buyerId).valueAsString()));
					row.putAll(system.getRowById(TableName.GoodTable, 
							row.get(RaceConfig.goodId).valueAsString()));
				}
				else if( !buyerKeys.isEmpty()) {
					// need query buyerTable
					row.putAll(system.getRowById(TableName.BuyerTable,
							row.get(RaceConfig.buyerId).valueAsString()));
				}
				if(!goodKeys.isEmpty()) {
					//System.out.println("weired query!");
					row.putAll(system.getRowById(TableName.GoodTable,
							row.get(RaceConfig.goodId).valueAsString()));
				}
				results.put(orderId, new ResultImpl(orderId, row.getKVs(keys)));
			} catch (TypeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }

    /**
     * 第三个查询
	 * 查询某位卖家某件商品所有订单的某些字段
	 * 
	 * @param salerid
	 *            卖家Id
	 * @param goodid
	 *            商品Id
	 * @param keys
	 *            待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
	 * @return 符合条件的订单集合，按照订单id从小至大排序
	 */
    @Override
    public Iterator<Result> call() throws Exception {
        // TODO
		// 根据商品ID找到多条订单信息 再筛选出keys 结果集按照订单id插入排序
    	ArrayList<String> buyerKeys = new ArrayList<String>();
    	ArrayList<String> goodKeys = new ArrayList<String>();
		if( keys != null) {
			for (String key : keys) {
				if (system.buyerAttrList.contains(key)) {
					buyerKeys.add(key);
				} else if (system.goodAttrList.contains(key)) {
					goodKeys.add(key);
				}
			}
		}
		
		TreeMap<Long, Result> results = new TreeMap<Long, Result>();
		Integer surrId = goodid.hashCode();
		if( surrId == 0) {
			return null;
		}
		else {
			// 在缓冲区里找对应的order数据
			List<Long> orderIds = system.rowCache.getFormIdCache(surrId, IdIndexType.GoodIdToOrderOffsets);
			if( orderIds != null) {
				// 找到了对应的orderid列表
				handleOffsets(orderIds, results,buyerKeys, goodKeys );
				
			}
			else {
				// 在索引里找offsetlist
				List<Long> offsetList = new ArrayList<Long>();
				for (FilePathWithIndex filePath : system.orderFileList) {
					
					DiskHashTable<Integer, List<Long>> hashTable = system.orderGoodIdIndexList
							.get(filePath.getFilePath());
					List<Long> offSetresults = hashTable.get(surrId);
					if (offSetresults.size() != 0) {
						// find the records offset
						// 找到后，按照降序插入TreeMap中
						offsetList.addAll(offSetresults);
					}

				}
				
				handleOffsets( offsetList, results, buyerKeys, goodKeys);
				// 放入缓冲区
				rowCache.putInIdCache(surrId, offsetList, IdIndexType.GoodIdToOrderOffsets);
			}
			
		}
		
		return results.values().iterator();
    }
}
