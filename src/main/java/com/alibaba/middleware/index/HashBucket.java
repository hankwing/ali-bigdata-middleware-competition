package com.alibaba.middleware.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.alibaba.middleware.conf.RaceConfig;

/**
 * 哈希索引使用的桶类  可有溢出桶
 * @author hankwing
 *
 * @param <K>
 * @param <V>
 */
public class HashBucket implements Serializable{

	private static final long serialVersionUID = 3610182543890121796L;
	private int bucketKey = 0;	// 缓冲区管理需要根据这个值调用context.writeBucket将桶写出去
	private int capacity  = 0;
	private int recordNum = 0;
	private TreeMap< String, Map<String,Long>> keyToAddress = null;		// need to write to disk
	private HashBucket nextBucket = null;
	private transient DiskHashTable context = null; 

	public HashBucket( DiskHashTable context, int bucketKey) {
		this.context = context;
		capacity = RaceConfig.hash_index_block_capacity;
		this.bucketKey = bucketKey;
		recordNum = 0;
		keyToAddress = new TreeMap<String, Map<String,Long>>(
				new ComparableKeys(String.valueOf(bucketKey).length() + 1));
	}
	
	public List<Map< String, Long>> getAllValues( String newBucketKey) {
		List<Map< String, Long>> allValues = new ArrayList<Map<String, Long>>();
		Map<String, Long> addMap = keyToAddress.get(newBucketKey);
		if( addMap != null) {
			allValues.add(keyToAddress.get(newBucketKey));
		}
		if( nextBucket != null ) {
			allValues.addAll(nextBucket.getAllValues( newBucketKey));
		}
		return allValues;
	}
	
	public HashBucket getNextBucket() {
		return nextBucket;
	}
	
	public void minusRecordNum() {
		recordNum -- ;
	}
	
	public Long getAddress( String bucketIndex, String key) {
		Map<String,Long> map = keyToAddress.get(bucketIndex);
		if( map == null) {
			if( nextBucket != null) {
				return nextBucket.getAddress(bucketIndex , key);
			}
			else {
				return null;
			}
		}
		else {
			return map.get(key);
		}
		
	}
	
	public void putAddress( String bucketIndex, String key, Long value) {
		if( recordNum + 1 > capacity) {
			if( nextBucket == null) {
				nextBucket = new HashBucket( null, bucketKey);	// 溢出桶无需管理
			}
			
			nextBucket.putAddress(bucketIndex, key, value);
		}
		else {
			recordNum ++;
			Map<String,Long> map = keyToAddress.get(bucketIndex);
			if( map == null) {
				map = new HashMap<String, Long>();
				keyToAddress.put(bucketIndex, map);
			}
			map.put(key, value);
		}
		
	}

	public int getBucketKey() {
		return this.bucketKey;
	}
	
}
