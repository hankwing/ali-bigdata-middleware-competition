package com.alibaba.middleware.threads;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdName;
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
	 */
    @SuppressWarnings("unchecked")
	@Override
    public KeyValueImpl call() {
        // TODO
    	//KeyValueImpl result = new KeyValueImpl();
    	List<String> keys = new ArrayList<String>();
    	Double sum = 0.0;
    	keys.add(key);
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
						Double orderId = 0.0;
						Row row = RecordsUtils.getRecordsByKeysFromFile(
								filePath.getFilePath(), keys, offset);
						if( row.getKV(RaceConfig.goodId).valueAsString().equals(goodid)) {
							try{
								row.getKV(key).valueAsString();
							} catch (RuntimeException e) {
								// 该条记录不存在这个key
								continue;
							} 
							
							// 该记录存在该key
							try {
								orderId = row.getKV(key).valueAsDouble();
							} catch (TypeException e) {
								// TODO Auto-generated catch block
								// 不是Double型的，当然也不是long型的
								return null;
							}
							sum += orderId;
							//results.put(orderId, new ResultImpl(orderId, row));
						}
					}
					
				}

			}
		}
		
		if( sum == 0) {
			return null;
		}
		else {
			return new KeyValueImpl(key, String.valueOf(sum));
		}
    }
}
