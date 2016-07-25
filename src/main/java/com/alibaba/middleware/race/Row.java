package com.alibaba.middleware.race;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;



/**
 * 其实就是个keyValueImpl的数组
 * @author hankwing
 *
 */
public class Row extends HashMap<String, KeyValueImpl> {

	private static final long serialVersionUID = -5133028627154935587L;

	public Row() {
		super();
	}

	public Row(KeyValueImpl kv) {
		super();
		this.put(kv.key(), kv);
	}

	public KeyValueImpl getKV(String key) {
		KeyValueImpl kv = this.get(key);
		//if (kv == null) {
		//	throw new RuntimeException(key + " is not exist");
		//}
		return kv;
	}

	public Row putKV(String key, String value) {
		KeyValueImpl kv = new KeyValueImpl(key, value);
		this.put(kv.key(), kv);
		return this;
	}
	
	public Row getKVs( Collection<String> keys) {
		
		Row row = new Row();
		if( keys == null) {
			return this;
		}
		else {
			for( String key : keys) {
				if( getKV(key) != null) {
					row.put(key, getKV(key));
				}
				
			}
			return row;
		}
		
	}

	public Row putKV(String key, long value) {
		KeyValueImpl kv = new KeyValueImpl(key, Long.toString(value));
		this.put(kv.key(), kv);
		return this;
	}
}
