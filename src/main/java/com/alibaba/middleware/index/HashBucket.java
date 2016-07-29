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
 * @param <T>
 */
public class HashBucket<K,T> implements Serializable{

	private static final long serialVersionUID = 3610182543890121796L;
	private int bucketKey = 0;	// 缓冲区管理需要根据这个值调用context.writeBucket将桶写出去
	private int capacity  = 0;
	public int recordNum = 0;
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
	
	public void writeSelfAfterBuilding() {
		//System.out.println("writeBucket:" + bucketKey);
		context.discardBucket(bucketKey);
	}
	
	public void writeSelfWhenBuilding() {
		context.writeBucketWhenBuilding(bucketKey);
	}
	
	public List<byte[]> getAddress(  String bucketIndex, K key) {
		List<byte[]> results = new ArrayList<byte[]>();
		Map<K, T> partialResult = keyToAddress.get(bucketIndex);
		if( partialResult != null  && partialResult.get(key) != null ) {
			if( classType == byte[].class) {
				results.add( (byte[]) partialResult.get(key));
			}
			else {
				results.addAll((Collection<? extends byte[]>) partialResult.get(key));
			}
			
		}
		
		if( nextBucket != null) {
			results.addAll(nextBucket.getAddress(bucketIndex, key));
		}
		return results;
		
	}
	
	public void putAddress( String bucketIndex, K key, byte[] value) {

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
				List<byte[]> valueList = (List<byte[]>) values.get(key);
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

		if( classType == byte[].class) {
			putAddress( bucketIndex, key, (byte[])value);
		}
		else if( recordNum + ((List<byte[]>) value).size() > capacity) {
			if( nextBucket == null) {
				nextBucket = new HashBucket<K,T>(context, 0, classType);	// 溢出桶无需管理
			}
			
			nextBucket.putAddress(bucketIndex, key, value);
		}
		else {
			recordNum += ((List<byte[]>) value).size();
			Map<K, List<byte[]>> values = (Map<K, List<byte[]>>) keyToAddress.get(bucketIndex);
			if(values == null) {
				values = new HashMap<K, List<byte[]>>();
				keyToAddress.put(bucketIndex, (Map<K, T>) values);
			}
			
			List<byte[]> valueList = values.get(key);
			if( valueList == null) {
				valueList = new ArrayList<byte[]>();
				values.put(key, valueList);
			}
			
			valueList.addAll(((List<byte[]>) value));
		}
		
	}

	public int getBucketKey() {
		return this.bucketKey;
	}
}
