package com.alibaba.middleware.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.conf.RaceConfig;

/**
 * 哈希索引使用的桶类  可有溢出桶
 * @author hankwing
 *
 * @param <K>
 * @param <V>
 */
public class HashBucket<K,V> implements Serializable{

	private static final long serialVersionUID = 3610182543890121796L;
	private int bucketKey = 0;	// 缓冲区管理需要根据这个值调用context.writeBucket将桶写出去
	private int capacity  = 0;
	private int recordNum = 0;
	private Map< K, V> keyToAddress = null;		// need to write to disk
	private HashBucket<K,V> nextBucket = null;
	private transient DiskHashTable context = null; 

	public HashBucket( DiskHashTable context, int bucketKey) {
		this.context = context;
		capacity = RaceConfig.hash_index_block_capacity;
		this.bucketKey = bucketKey;
		recordNum = 0;
		keyToAddress = new HashMap<K, V>();
	}
	
	public List<Map< K, V>> getAllValues() {
		List<Map< K, V>> allValues = new ArrayList<Map<K, V>>();
		allValues.add(keyToAddress);
		if( nextBucket != null ) {
			allValues.addAll(nextBucket.getAllValues());
		}
		return allValues;
	}
	
	public V getAddress( K key) {
		if( keyToAddress.get(key) == null) {
			if( nextBucket != null) {
				return nextBucket.getAddress(key);
			}
			else {
				return null;
			}
		}
		else {
			return keyToAddress.get(key);
		}
		
	}
	
	public void putAddress( K key, V value) {
		if( recordNum + 1 > capacity) {
			if( nextBucket == null) {
				nextBucket = new HashBucket<K, V>( null, 0);	// 溢出桶无需管理
			}
			
			nextBucket.putAddress(key, value);
		}
		else {
			recordNum ++;
			keyToAddress.put(key, value);
		}
		
	}

	public int getBucketKey() {
		return this.bucketKey;
	}
	
}
