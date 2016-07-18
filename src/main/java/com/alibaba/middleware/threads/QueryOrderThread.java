package com.alibaba.middleware.threads;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.race.OrderSystem.Result;
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

    @Override
    public ResultImpl call() throws Exception {
        // TODO
    	ResultImpl result = null;
		Row resultKV = new Row();
		resultKV.putKV(RaceConfig.orderId, orderId);
		result = new ResultImpl(orderId, resultKV);
		if (keys == null) {
			// 为Null 查询所有字段
			resultKV.putAll(system.getRowById(TableName.OrderTable, IdName.OrderId,
					orderId, keys));
			resultKV.putAll(system.getRowById(TableName.BuyerTable, IdName.BuyerId,
					resultKV.get(RaceConfig.buyerId).valueAsString(), keys));
			resultKV.putAll(system.getRowById(TableName.GoodTable, IdName.GoodId,
					resultKV.get(RaceConfig.goodId).valueAsString(), keys));
		} else if ( !keys.isEmpty()) {
			// 查询指定字段
			List<String> orderKeys = new ArrayList<String>();
			List<String> buyerKesy = new ArrayList<String>();
			List<String> goodKeys = new ArrayList<String>();
			for (String key : keys) {
				if (system.orderAttrList.contains(key)) {
					orderKeys.add(key);
				} else if (system.buyerAttrList.contains(key)) {
					buyerKesy.add(key);
				} else if (system.goodAttrList.contains(key)) {
					goodKeys.add(key);
				}
			}
			resultKV.putAll(system.getRowById(TableName.OrderTable, IdName.OrderId,
					orderId, orderKeys));
			resultKV.putAll(system.getRowById(TableName.BuyerTable, IdName.BuyerId,
					resultKV.get(RaceConfig.buyerId).valueAsString(), buyerKesy));
			resultKV.putAll(system.getRowById(TableName.GoodTable, IdName.GoodId,
					resultKV.get(RaceConfig.goodId).valueAsString(), goodKeys));
		}
		result = new ResultImpl(orderId, resultKV);
        return result;
    }
}
