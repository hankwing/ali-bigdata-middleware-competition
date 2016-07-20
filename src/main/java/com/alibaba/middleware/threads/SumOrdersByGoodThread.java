package com.alibaba.middleware.threads;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.KeyValueImpl;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.ResultImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

/**
 * @author Jelly
 */
public class SumOrdersByGoodThread extends QueryThread<KeyValueImpl> {

    private String goodid;
    private String key;
    private OrderSystemImpl system = null;

    public SumOrdersByGoodThread(OrderSystemImpl system, String goodid, String key) {
    	this.system = system;
        this.goodid = goodid;
        this.key = key;
    }

    /**
     * 第四个查询
	 * 对某件商品的某个字段求和，只允许对long和double类型的KV求和 如果字段中既有long又有double，则使用double
	 * 如果求和的key中包含非long/double类型字段，则返回null 如果查询订单中的所有商品均不包含该字段，则返回null
	 * 
	 * @param goodid
	 *            商品Id
	 * @param key
	 *            求和字段
	 * @return 求和结果
     * @throws TypeException 
	 */
    @SuppressWarnings("unchecked")
	@Override
    public KeyValueImpl call() throws TypeException {
        // TODO
    	//KeyValueImpl result = new KeyValueImpl()
		
    	List<String> keys = new ArrayList<String>();
    	boolean isFound = false;
    	Long longSum = 0L;
    	Double doubleSum = 0.0;
    	keys.add(key);
    	List<String> orderKeys = new ArrayList<String>();
		List<String> buyerKeys = new ArrayList<String>();
		List<String> goodKeys = new ArrayList<String>();
		for (String key : keys) {
			if (system.orderAttrList.contains(key)) {
				orderKeys.add(key);
			} else if (system.buyerAttrList.contains(key)) {
				buyerKeys.add(key);
			} else if (system.goodAttrList.contains(key)) {
				goodKeys.add(key);
			}
		}
		
		Integer surrId = goodid.hashCode();
		if( surrId == 0) {
			return null;
		}
		else {
			for (FilePathWithIndex filePath : system.orderFileList) {
				DiskHashTable<Integer, List<Long>> hashTable = system.orderGoodIdIndexList
						.get(filePath.getFilePath());
				if (hashTable == null) {
					hashTable = system.getHashDiskTable(filePath.getFilePath(),
							filePath.getOrderGoodIdIndex());
					system.orderGoodIdIndexList.put(filePath.getFilePath(), hashTable);
				}
				long resultNum = hashTable.get(surrId).size();
				if (resultNum != 0) {
					// find the records offset
					// 找到后，按照降序插入TreeMap中
					System.out.println("records offset:"
							+ resultNum);
					for( Long offset: hashTable.get(surrId)) {
						long orderid = 0;
						long longValue = 0;
						Double doubleValue = 0.0;
						boolean isLong = true;
						// 现在缓冲区里找
						boolean isCacheHit = false;
						Row row = system.rowCache.getFromCache(offset + filePath.getFilePath().hashCode(),
								TableName.GoodTable);
						if(row != null) {
							isCacheHit = true;
							row = row.getKV(RaceConfig.goodId).valueAsString().equals(goodid) ?
									row : RecordsUtils.getRecordsByKeysFromFile(
											filePath.getFilePath(), keys, offset);
						}
						else {
							row = RecordsUtils.getRecordsByKeysFromFile(
									filePath.getFilePath(), keys, offset);
						}
						
						if( row.getKV(RaceConfig.goodId).valueAsString().equals(goodid)) {
							orderid = row.getKV(RaceConfig.orderId).valueAsLong();
							// 放入缓冲区
							if( !isCacheHit ) {
								system.rowCache.putInCache(offset + filePath.getFilePath().hashCode()
										, row, TableName.GoodTable);
							}
							
							if(!buyerKeys.isEmpty()) {
								// need query buyerTable 
								row.putAll(system.getRowById(TableName.BuyerTable, RaceConfig.buyerId,
										row.get(RaceConfig.buyerId).valueAsString(), buyerKeys));	
							}
							if( !goodKeys.isEmpty()) {
								// 此时说明此key就在buyerTable中
								row.putAll(system.getRowById(TableName.GoodTable, RaceConfig.goodId,
										row.get(RaceConfig.goodId).valueAsString(), goodKeys));
								try {
									isFound = true;
									longValue = row.getKV(key).valueAsLong();
								} catch (TypeException e) {
									// TODO Auto-generated catch block
									// 不是long型的
									try {
										isLong = false;
										doubleValue = row.getKV(key).valueAsDouble();
									} catch (TypeException e2) {
										// TODO Auto-generated catch block
										// 不是Double型的 返回Null
										return null;
									}
								}
								if( isLong) {
									return new KeyValueImpl(key, String.valueOf(longValue * resultNum));
								}
								else {
									return new KeyValueImpl(
											key, String.format("%.10f", doubleValue * resultNum));
								}
							}
							
							try{
								row.getKV(key).valueAsString();
							} catch (RuntimeException e) {
								// 该条记录不存在这个key
								continue;
							} 
							
							// 该记录存在该key
							try {
								isFound = true;
								longValue = row.getKV(key).valueAsLong();
							} catch (TypeException e) {
								// TODO Auto-generated catch block
								// 不是long型的
								try {
									isLong = false;
									doubleValue = row.getKV(key).valueAsDouble();
								} catch (TypeException e2) {
									// TODO Auto-generated catch block
									// 不是Double型的 返回Null
									return null;
								}
							}
							if( isLong) {
								longSum += longValue;
							}
							else {
								doubleSum += doubleValue;
							}
							
							//results.put(orderId, new ResultImpl(orderId, row));
						}
					}
					
				}

			}
		}
		
		if( longSum == 0 && doubleSum == 0 && !isFound) {
			return null;
		}
		else if( doubleSum == 0) {
			// 返回long 值
			return new KeyValueImpl(key, String.valueOf(longSum));
		}
		else{
			Double doubleReturn = doubleSum + longSum;
			return new KeyValueImpl(key, String.format("%.10f", doubleReturn));
		}
    }
}
