package com.alibaba.middleware.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
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
public class HashBucket<K,T> implements Serializable{

	private static final long serialVersionUID = 3610182543890121796L;
	private int bucketKey = 0;	// 缓冲区管理需要根据这个值调用context.writeBucket将桶写出去
	private int capacity  = 0;
	private int recordNum = 0;
	private Map< String, Map<K, T>> keyToAddress = null;		// need to write to disk
	private HashBucket<K,T> nextBucket = null;
	private transient DiskHashTable<K,T> context = null; 
	private Class<?> classType = null;

	public HashBucket( DiskHashTable<K,T> context, int bucketKey, Class<?> classType ) {
		this.context = context;
		this.classType = classType;
		capacity = RaceConfig.hash_index_block_capacity;
		this.bucketKey = bucketKey;
		recordNum = 0;
		keyToAddress = new TreeMap<String, Map<K,T>>(
				new ComparableKeys(String.valueOf(bucketKey).length() + 1));
	}
	
	public List<Map<K, T>> getAllValues(String newBucketKey) {
		
		List<Map< K, T>> allValues = new ArrayList<Map<K, T>>();
		Map<K, T> addMap = keyToAddress.get(newBucketKey);
		if( addMap != null) {
			allValues.add(addMap);
		}
		if( nextBucket != null ) {
			allValues.addAll(nextBucket.getAllValues( newBucketKey));
		}
		return allValues;
	}
	
	public HashBucket<K,T> getNextBucket() {
		return nextBucket;
	}
	
	public void minusRecordNum( int number) {
		recordNum -= number ;
	}
	
	public DiskHashTable<K,T> getContext() {
		return context;
	}
	
	public void setContext( DiskHashTable<K,T> context) {
		this.context = context;
	}
	
	public void writeSelf() {
		//System.out.println("writeBucket:" + bucketKey);
		context.writeBucket(bucketKey);
	}
	
	public List<Long> getAddress(  String bucketIndex, K key) {
		List<Long> results = new ArrayList<Long>();
		Map<K, T> partialResult = keyToAddress.get(bucketIndex);
		if( partialResult != null  && partialResult.get(key) != null ) {
			if( classType == Long.class) {
				results.add((Long) partialResult.get(key));
			}
			else {
				results.addAll((Collection<? extends Long>) partialResult.get(key));
			}
			
		}
		
		if( nextBucket != null) {
			results.addAll(nextBucket.getAddress(bucketIndex, key));
		}
		return results;
		
	}
	
	public void putAddress( String bucketIndex, K key, Long value) {

		if( recordNum + 1 > capacity) {
			if( nextBucket == null) {
				nextBucket = new HashBucket<K,T>( context, 0, classType);	// 溢出桶无需管理
			}
			
			nextBucket.putAddress( bucketIndex, key, value);
		}
		else {
			recordNum ++;
			Map<K,T> values = keyToAddress.get(bucketIndex);
			if(values == null) {
				values = new HashMap<K,T>();
				keyToAddress.put(bucketIndex, values);
				
			}
			
			if( classType == List.class) {
				List<Long> valueList = (List<Long>) values.get(key);
				if(valueList == null) {
					valueList = new ArrayList();
					values.put(key, (T) valueList);
				}
				valueList.add(value);
			}
			else {
				values.put(key, (T) value);
			}
			
		}
		
	}
	
	public void putAddress( String bucketIndex, K key, T value) {

		if( classType == Long.class) {
			putAddress( bucketIndex, key, (Long)value);
		}
		else if( recordNum + ((List<Long>) value).size() > capacity) {
			if( nextBucket == null) {
				nextBucket = new HashBucket<K,T>(context, 0, classType);	// 溢出桶无需管理
			}
			
			nextBucket.putAddress(bucketIndex, key, value);
		}
		else {
			recordNum += ((List<Long>) value).size();
			Map<K, List<Long>> values = (Map<K, List<Long>>) keyToAddress.get(bucketIndex);
			if(values == null) {
				values = new HashMap<K, List<Long>>();
				keyToAddress.put(bucketIndex, (Map<K, T>) values);
			}
			
			List<Long> valueList = values.get(key);
			if( valueList == null) {
				valueList = new ArrayList<Long>();
				values.put(key, valueList);
			}
			
			valueList.addAll(((List<Long>) value));
		}
		
	}

	public int getBucketKey() {
		return this.bucketKey;
	}
	
}
