package com.alibaba.middleware.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.handlefile.FileIndexWithOffset;
import com.alibaba.middleware.race.KeyValueImpl;
import com.alibaba.middleware.race.Row;


public class RecordsUtils {
	
	/**
	 * 判断字符串是否是长整数
	 */
	public static boolean isLong(String value) {
		try {
			Long.parseLong(value);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	/**
	 * 判断字符串是否是浮点数
	 */
	public static boolean isDouble(String value) {
		try {
			Double.parseDouble(value);
			if (value.contains("."))
				return true;
			return false;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	/**
	 * 判断字符串是否是可加
	 */
	public static boolean isCanSum(String value) {
		return  isDouble(value) || isLong(value);
	}

	/***
	 * get value from Record
	 * @param record
	 * @param key
	 * @return
	 */
	/*public static String getValueFromRecord(String record, String key){
		String[] kvs = record.split("\t");
		String value = null;
		for(int i = 0; i< kvs.length ; i++){
			String str = kvs[i];
			int p = str.indexOf(":");
			String kstr = str.substring(0 , p);
			String vstr = str.substring(p+1);
			if (kstr.length() == 0 || vstr.length() == 0) {
				throw new RuntimeException("Bad data:" + record);
			}
			if(kstr.equals(key)) {
				value = vstr;
				break;
			}
		}
		return value;
	}*/
	
	public static Row getRecordsByKeysFromFile(String fileName,Collection<String> keys,Long offset){
		Row row = new Row();
		try {
				BufferedReader reader = new BufferedReader(new FileReader(fileName));
				reader.skip(offset);
				String record = reader.readLine();

				String[] kvs = record.split("\t");

				for(int i = 0; i<kvs.length ;i++){
					String str = new String(kvs[i]);
					int p = str.indexOf(":");
					String kstr = str.substring(0 , p);
					String vstr = str.substring(p+1);
					if (kstr.length() == 0 || vstr.length() == 0) {
						throw new RuntimeException("Bad data:" + record);
					}
					if(keys == null || kstr.equals(RaceConfig.orderId) || kstr.equals(RaceConfig.buyerId) 
							|| kstr.equals(RaceConfig.goodId) || keys.contains(kstr)) {
						row.putKV(kstr, vstr);
					}
					
				}
				
				reader.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		return row;
	}
	
	/**
	 * 从文件里一次读取一行数据出来
	 * @param fileName
	 * @param offset
	 * @return
	 */
	public static String getStringFromFile(LinkedBlockingQueue<RandomAccessFile> file
			, Long offset, TableName tableType){
		String result = null;
		
		try {
			RandomAccessFile fileReader = file.take();
			fileReader.seek(offset);
			result = new String(fileReader.readLine().getBytes(StandardCharsets.ISO_8859_1), 
					StandardCharsets.UTF_8);
			// 放回文件句柄队列中
			file.add(fileReader);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		return result;
		
		
	}
	
	/**
	 * 将文件地址下标+offset编码为byte数组
	 * @param dataSerialNumber
	 * @param offset
	 * @return
	 */
	public static byte[] encodeIndex(int dataSerialNumber, long offset){
		ByteBuffer buffer = ByteBuffer.allocate(12);
		//放入源数据文件编号
		buffer.putInt(dataSerialNumber);
		buffer.putLong(offset);
		//buffer.clear();
		return buffer.array();
	}
	
	/**
	 * 解析indexBytes为文件下标+offset
	 * @param dataFileMapping
	 * @param indexBytes
	 */
	public static FileIndexWithOffset decodeIndex(byte[] indexBytes){
		ByteBuffer buffer = ByteBuffer.wrap(indexBytes);
		return new FileIndexWithOffset( buffer.getInt(), buffer.getLong());
	}
	
	/**
	 * 得到一行数据里某个key的value 用于good和buyer表
	 * @param line
	 * @return
	 */
	public static Integer getValueFromLineWithKeyList(
			String line, String targetKey, HashSet<String> keyList) {
		if( line != null) {
			int keyHashCode = 0;
			//Row kvMap = new Row();
			String[] kvs = line.split("\t");
			for (String rawkv : kvs) {
				int p = rawkv.indexOf(':');
				String key = rawkv.substring(0, p);
				String value = rawkv.substring(p + 1);
				keyList.add(key);
				if( key.equals(targetKey)) {
					// 找到了所需的key
					keyHashCode = value.hashCode();
				}
				/*if (key.length() == 0 || value.length() == 0) {
					throw new RuntimeException("Bad data:" + line);
				}*/
				
			}
			return keyHashCode;
		}
		else {
			return null;
		}
	}
	
	/**
	 * 得到一行数据里某个key的value，并且加入key到列表中
	 * @param line
	 * @return
	 */
	public static String getValueFromLine(String line, String key) {
		if( line != null) {
			int location = line.indexOf(key);
			int endLocation = line.indexOf("\t", location);
			return line.substring(location + key.length() + 1,
					endLocation != -1? endLocation:line.length() );
		}
		else {
			return null;
		}
	}
	
	/**
	 * 工具类  可从一行数据中解析出KeyValue对
	 * @param line
	 * @return
	 */
	public static Row createKVMapFromLine(String line) {
		if( line != null && !line.equals("")) {
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
	
	/**
	 * 工具类  可从一行数据中解析出KeyValue对
	 * @param line
	 * @return
	 */
	public static Row createSubKVMapFromLine(String line, Collection<String> keys) {
		if( line != null && !line.equals("")) {
			Row kvMap = new Row();
			String[] kvs = line.split("\t");
			
			for (String rawkv : kvs) {
				int p = rawkv.indexOf(':');
				String key = rawkv.substring(0, p);
				String value = rawkv.substring(p + 1);
				if (key.length() == 0 || value.length() == 0) {
					throw new RuntimeException("Bad data:" + line);
				}
				if( keys == null || keys.contains(key)) {
					kvMap.put(key, new KeyValueImpl(key, value));
				}
			}
			return kvMap;
		}
		else {
			return null;
		}
	}
	
	/**
	 * 获得一条记录的主键
	 * @param record
	 * @return
	 *//*
	public static String getIdFromRecord(String record){
		String[] kvs = record.split("\t");
		String str = new String(kvs[0]);
		int p = str.indexOf(":");
		String vstr = str.substring(p+1);
		return vstr;
	}
	
	//从KeyValue中获取Value
	public static String getValueFromKV(String keyValue){
		String[] kvs = keyValue.split(":");
		return kvs[1];
	}*/
}
