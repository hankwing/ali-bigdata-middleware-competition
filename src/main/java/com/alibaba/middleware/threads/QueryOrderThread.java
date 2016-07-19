package com.alibaba.middleware.threads;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.ResultImpl;
import com.alibaba.middleware.race.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Jelly
 */
public class QueryOrderThread extends QueryThread<ResultImpl> {
    private long orderId;
    private Collection<String> keys;
    private OrderSystemImpl system = null;

    public QueryOrderThread(OrderSystemImpl system, long orderId, Collection<String> keys) {
    	this.system = system;
        this.orderId = orderId;
        this.keys = keys;
    }

    /**
	 * 查询订单号为orderid的指定字段
	 * 
	 * @param orderId
	 *            订单号
	 * @param keys
	 *            待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
	 * @return 查询结果，如果该订单不存在，返回null
	 */
    @Override
    public ResultImpl call() {
    	ResultImpl result = null;
		Row resultKV = new Row();
		resultKV.putKV(RaceConfig.orderId, orderId);
		if (keys == null) {
			Row orderIdRow = system.getRowById(TableName.OrderTable,RaceConfig.orderId,
					orderId, keys);
			if( !orderIdRow.isEmpty() ) {
				resultKV.putAll(orderIdRow);
				resultKV.putAll(system.getRowById(TableName.BuyerTable, RaceConfig.buyerId,
						resultKV.get(RaceConfig.buyerId).valueAsString(), keys));
				resultKV.putAll(system.getRowById(TableName.GoodTable, RaceConfig.goodId,
						resultKV.get(RaceConfig.goodId).valueAsString(), keys));
			}
			else {
				// 没有找到对应orderid的记录
				return null;
			}
		} else if ( !keys.isEmpty()) {
			// 查询指定字段
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
			Row orderIdRow = system.getRowById(TableName.OrderTable, RaceConfig.orderId,
					orderId, orderKeys);
			if( !orderIdRow.isEmpty() ) {
				resultKV.putAll(orderIdRow);
				resultKV.putAll(system.getRowById(TableName.BuyerTable, RaceConfig.buyerId,
						resultKV.get(RaceConfig.buyerId).valueAsString(), buyerKeys));
				resultKV.putAll(system.getRowById(TableName.GoodTable, RaceConfig.goodId,
						resultKV.get(RaceConfig.goodId).valueAsString(), goodKeys));
			}
			else {
				// 没有找到对应orderid的记录
				return null;
			}
			
		}
		result = new ResultImpl(orderId, resultKV.getKVs(keys));
		
        return result;
    }

}
