package com.alibaba.middleware.threads;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.race.OrderSystem.TypeException;
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
     * 第一个查询
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
		
		try {
			resultKV.putKV(RaceConfig.orderId, orderId);
			if (keys == null) {
				// 需要返回所有记录
				Row orderIdRow = system.getRowById(TableName.OrderTable,orderId);
				if( orderIdRow !=null ) {
					resultKV.putAll(orderIdRow);
					resultKV.putAll(system.getRowById(TableName.BuyerTable,
							resultKV.get(RaceConfig.buyerId).valueAsString()));
					resultKV.putAll(system.getRowById(TableName.GoodTable,
							resultKV.get(RaceConfig.goodId).valueAsString()));
				}
				else {
					// 没有找到对应orderid的记录
					return null;
				}
			} else if ( !keys.isEmpty()) {
				// 查询指定字段
				// 先找到要查询的key在哪个表里出现了
				//List<String> orderKeys = new ArrayList<String>();
				List<String> buyerKeys = new ArrayList<String>();
				List<String> goodKeys = new ArrayList<String>();
				for (String key : keys) {
					if (system.buyerAttrList.contains(key)) {
						buyerKeys.add(key);
					} else if (system.goodAttrList.contains(key)) {
						goodKeys.add(key);
					}
				}
				Row orderIdRow = system.getRowById(TableName.OrderTable,orderId);
				if( orderIdRow !=null ) {
					resultKV.putAll(orderIdRow);
					if(!buyerKeys.isEmpty()) {
						// 需要查询buyer表
						resultKV.putAll(system.getRowById(TableName.BuyerTable,
								resultKV.get(RaceConfig.buyerId).valueAsString()));
					}
					if( !goodKeys.isEmpty()) {
						// 需要查询good表
						resultKV.putAll(system.getRowById(TableName.GoodTable,
								resultKV.get(RaceConfig.goodId).valueAsString()));
					}
				}
				else {
					// 没有找到对应orderid的记录
					return null;
				}
				
			}
			else {
				// 这里说明key为空 只需要判断是否存在该条记录即可
				// 这里需改进
				
				if(system.getRowById(TableName.OrderTable, orderId) == null) {
					// 没找到相应订单
					return null;
				}
				
			}
		} catch (TypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		result = new ResultImpl(orderId, resultKV.getKVs(keys));
		
        return result;
    }

}
