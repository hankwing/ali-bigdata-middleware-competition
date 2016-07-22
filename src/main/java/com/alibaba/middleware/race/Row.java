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
	
	/**
	 * 工具类  可从一行数据中解析出KeyValue对
	 * @param line
	 * @return
	 */
	public static Row createKVMapFromLine(String line) {
		if( line != null) {
			Row kvMap = new Row();
			String[] kvs = line.split("\t");
			
			for (String rawkv : kvs) {
				int p = rawkv.indexOf(':');
				String key = rawkv.substring(0, p);
				String value = rawkv.substring(p + 1);
				if (key.length() == 0 || value.length() == 0) {
					throw new RuntimeException("Bad data:" + line);
				}
				kvMap.put(key, new KeyValueImpl(key, value));
			}
			return kvMap;
		}
		else {
			return null;
		}
	}
}
