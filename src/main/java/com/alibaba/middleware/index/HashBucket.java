package com.alibaba.middleware.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.tools.IndexBucketKey;

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
	private Map< String, Map<String, List<Long>>> keyToAddress = null;		// need to write to disk
	private HashBucket nextBucket = null;
	private transient DiskHashTable context = null; 

	public HashBucket( DiskHashTable context, int bucketKey) {
		this.context = context;
		capacity = RaceConfig.hash_index_block_capacity;
		this.bucketKey = bucketKey;
		recordNum = 0;
		keyToAddress = new TreeMap<String, Map<String,List<Long>>>(
				new ComparableKeys(String.valueOf(bucketKey).length() + 1));
	}
	
	public List<Map<String, List<Long>>> getAllValues(String newBucketKey) {
		
		List<Map< String, List<Long>>> allValues = new ArrayList<Map<String, List<Long>>>();
		Map<String, List<Long>> addMap = keyToAddress.get(newBucketKey);
		if( addMap != null) {
			allValues.add(addMap);
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
	
	public List<Long> getAddress(  String bucketIndex, String key) {
		List<Long> results = new ArrayList<Long>();
		Map<String, List<Long>> partialResult = keyToAddress.get(bucketIndex);
		if( partialResult != null  && partialResult.get(key) != null ) {
			results.addAll(partialResult.get(key));
		}
		
		if( nextBucket != null) {
			results.addAll(nextBucket.getAddress(bucketIndex, key));
		}
		return results;
		
	}
	
	public void putAddress( String bucketIndex, String key, Long value) {

		if( recordNum + 1 > capacity) {
			if( nextBucket == null) {
				nextBucket = new HashBucket( context, 0);	// 溢出桶无需管理
			}
			
			nextBucket.putAddress( bucketIndex, key, value);
		}
		else {
			recordNum ++;
			Map<String,List<Long>> values = keyToAddress.get(bucketIndex);
			if(values == null) {
				values = new HashMap<String,List<Long>>();
				keyToAddress.put(bucketIndex, values);
				
			}
			List<Long> valueList = values.get(key);
			if(valueList == null) {
				valueList = new ArrayList<Long>();
				values.put(key, valueList);
			}
			valueList.add(value);
			
		}
		
	}
	
	public void putAddress( String bucketIndex, String key, List<Long> value) {
		if( recordNum + value.size() > capacity) {
			if( nextBucket == null) {
				nextBucket = new HashBucket(context, 0);	// 溢出桶无需管理
			}
			
			nextBucket.putAddress(bucketIndex, key, value);
		}
		else {
			recordNum += value.size();
			Map<String, List<Long>> values = keyToAddress.get(bucketIndex);
			if(values == null) {
				values = new HashMap<String, List<Long>>();
				keyToAddress.put(bucketIndex, values);
			}
			
			List<Long> valueList = values.get(key);
			if( valueList == null) {
				valueList = new ArrayList<Long>();
				values.put(key, valueList);
			}
			
			valueList.addAll(value);
		}
		
	}
	
}
