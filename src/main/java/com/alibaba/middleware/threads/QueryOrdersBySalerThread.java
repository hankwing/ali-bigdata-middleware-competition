package com.alibaba.middleware.threads;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystem;
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

    public QueryOrdersBySalerThread(OrderSystemImpl system,
    		String salerid, String goodid, Collection<String> keys) {
    	this.system = system;
        this.salerid = salerid;
        this.goodid = goodid;
        this.keys = keys;
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
		List<String> orderKeys = new ArrayList<String>();
		List<String> buyerKeys = new ArrayList<String>();
		List<String> goodKeys = new ArrayList<String>();
		if( keys == null ) {
			orderKeys = null;
			buyerKeys = null;
			goodKeys = null;
		}
		else {
			for (String key : keys) {
				if (system.orderAttrList.contains(key)) {
					orderKeys.add(key);
				} else if (system.buyerAttrList.contains(key)) {
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
			
			for (FilePathWithIndex filePath : system.orderFileList) {
				DiskHashTable<Integer, List<Long>> hashTable = system.orderGoodIdIndexList
						.get(filePath.getFilePath());
				if (hashTable == null) {
					System.out.println("weired...");
					hashTable = system.getHashDiskTable(filePath.getFilePath(),
							filePath.getOrderGoodIdIndex());
					system.orderGoodIdIndexList.put(filePath.getFilePath(), hashTable);
				}
				long resultNum = hashTable.get(surrId).size();
				if (resultNum != 0) {
					// find the records offset
					// 找到后，按照降序插入TreeMap中
					
					for( Long offset: hashTable.get(surrId)) {
						// 现在缓冲区里找
						Row row = system.rowCache.getFromCache(offset + filePath.getFilePath().hashCode(),
								TableName.OrderTable);
						if(row != null) {
							row = row.getKV(RaceConfig.goodId).valueAsString().equals(goodid) ?
									row : Row.createKVMapFromLine(RecordsUtils.getStringFromFile(
											filePath, offset, TableName.OrderTable));
						}
						else {
							row = Row.createKVMapFromLine(RecordsUtils.getStringFromFile(
									filePath, offset, TableName.OrderTable));
						}
						
						if( row.getKV(RaceConfig.goodId).valueAsString().equals(goodid)) {
							
							long orderId = row.getKV(RaceConfig.orderId).valueAsLong();
							if( !buyerKeys.isEmpty()) {
								// need query buyerTable
								row.putAll(system.getRowById(TableName.BuyerTable, RaceConfig.buyerId,
										row.get(RaceConfig.buyerId).valueAsString()));
							}
							if(!goodKeys.isEmpty()) {
								//System.out.println("weired query!");
								row.putAll(system.getRowById(TableName.GoodTable, RaceConfig.goodId,
										row.get(RaceConfig.goodId).valueAsString()));
							}
							results.put(orderId, new ResultImpl(orderId, row.getKVs(keys)));
							
						}				
						
					}
					
				}

			}
		}
		
		return results.values().iterator();
    }
}
