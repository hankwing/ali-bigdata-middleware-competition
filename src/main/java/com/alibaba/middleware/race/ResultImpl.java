package com.alibaba.middleware.race;

import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;

public class ResultImpl implements Result{

	private long orderid;
	private Row kvMap;
	
	public ResultImpl( long orderid, Row kv) {
		this.orderid = orderid;
		this.kvMap = kv;
	}
	
	@Override
	public KeyValue get(String key) {
		// TODO Auto-generated method stub
		return this.kvMap.get(key);
	}

	@Override
	public KeyValue[] getAll() {
		// TODO Auto-generated method stub
		return kvMap.values().toArray(new KeyValue[0]);
	}

	@Override
	public long orderId() {
		// TODO Auto-generated method stub
		return orderid;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("orderid: " + orderid + " {");
		if (kvMap != null && !kvMap.isEmpty()) {
			for (KeyValueImpl kv : kvMap.values()) {
				sb.append(kv.toString());
				sb.append(",\n");
			}
		}
		sb.append('}');
		return sb.toString();
	}

}
