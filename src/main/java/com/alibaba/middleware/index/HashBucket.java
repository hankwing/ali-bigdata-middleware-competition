package com.alibaba.middleware.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
public class HashBucket<K> implements Serializable{

	private static final long serialVersionUID = 3610182543890121796L;
	private int bucketKey = 0;	// 缓冲区管理需要根据这个值调用context.writeBucket将桶写出去
	private int capacity  = 0;
	public int recordNum = 0;
	private Map< String, Map<K, byte[]>> keyToAddress = null;		// need to write to disk
	private HashBucket<K> nextBucket = null;
	private transient DiskHashTable<K> context = null; 
	//private Class<?> classType = null;
	
	public HashBucket() { }			// 弄个无参构造函数序列化比较快

	public HashBucket( DiskHashTable<K> context, int bucketKey ) {
		this.context = context;
		//this.classType = classType;
		capacity = RaceConfig.hash_index_block_capacity;
		this.bucketKey = bucketKey;
		recordNum = 0;
		keyToAddress = new TreeMap<String, Map<K,byte[]>>(
				new ComparableKeys(String.valueOf(bucketKey).length() + 1));
	}
	
	/**
	 * 将buyer和good表的所有索引值的标志位置0
	 */
	public void resetAllValuesSigns() {
		try{
			for( Map<K,byte[]> map : keyToAddress.values()) {
				for( byte[] bytes : map.values()) {
					// 标志位置0
					bytes[0] = 0;
				}
			}
			if( nextBucket != null) {
				nextBucket.resetAllValuesSigns();
			}
		} catch( Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public Map<K, byte[]> getAllValues(String newBucketKey) {
		
		Map< K, byte[]> allValues = new HashMap<K, byte[]>();
		Map<K, byte[]> addMap = keyToAddress.get(newBucketKey);
		if( addMap != null) {
			allValues.putAll(addMap);
		}
		if( nextBucket != null ) {
			allValues.putAll(nextBucket.getAllValues( newBucketKey));
		}
		return allValues;
	}
	
	public HashBucket<K> getNextBucket() {
		return nextBucket;
	}
	
	public void minusRecordNum( int number) {
		recordNum -= number ;
	}
	
	public DiskHashTable<K> getContext() {
		return context;
	}
	
	public void setContext( DiskHashTable<K> context) {
		this.context = context;
	}
	
	public void writeSelfAfterBuilding() {
		//System.out.println("writeBucket:" + bucketKey);
		context.discardBucket(bucketKey);
	}
	
	public void writeSelfWhenBuilding() {
		context.writeBucketWhenBuilding(bucketKey);
	}
	
	/**
	 * 无则返回null
	 * @param bucketIndex
	 * @param key
	 * @return
	 */
	public byte[] getAddress(  String bucketIndex, K key) {

		Map<K, byte[]> partialResult = keyToAddress.get(bucketIndex);
		if( partialResult != null && partialResult.get(key) != null) {
			return partialResult.get(key);
		}
		else if( nextBucket != null ){
			return nextBucket.getAddress(bucketIndex, key);
		}
		return null;
		
	}
	
	/**
	 * 直接替换相应key里的byte数组
	 * @param bucketIndex
	 * @param key
	 * @param value
	 */
	public void replaceAddress( String bucketIndex, K key, byte[] newvalue) {

		Map<K,byte[]> values = keyToAddress.get(bucketIndex);
		if( values != null && values.get(key) != null) {
			values.put(key, newvalue);
		}
		else {
			nextBucket.replaceAddress(bucketIndex, key, newvalue);
		}
		
	}
	
	public void putAddress( String bucketIndex, K key, byte[] value) {

		if( recordNum + 1 > capacity) {
			if( nextBucket == null) {
				nextBucket = new HashBucket<K>( context, 0);	// 溢出桶无需管理
			}
			
			nextBucket.putAddress( bucketIndex, key, value);
		}
		else {
			recordNum ++;
			Map<K,byte[]> values = keyToAddress.get(bucketIndex);
			if(values == null) {
				values = new HashMap<K,byte[]>();
				keyToAddress.put(bucketIndex, values);
				
			}
			values.put(key, value);
		}
		
	}
	
	/*public void putAddress( String bucketIndex, K key, T value) {

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
		
	}*/

	public int getBucketKey() {
		return this.bucketKey;
	}
}
