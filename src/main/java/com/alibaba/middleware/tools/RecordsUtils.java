package com.alibaba.middleware.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.handlefile.FileIndexWithOffset;
import com.alibaba.middleware.index.ByteDirectMemory;
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
		return isDouble(value) || isLong(value);
	}

	/***
	 * get value from Record
	 * 
	 * @param record
	 * @param key
	 * @return
	 */
	/*
	 * public static String getValueFromRecord(String record, String key){
	 * String[] kvs = record.split("\t"); String value = null; for(int i = 0; i<
	 * kvs.length ; i++){ String str = kvs[i]; int p = str.indexOf(":"); String
	 * kstr = str.substring(0 , p); String vstr = str.substring(p+1); if
	 * (kstr.length() == 0 || vstr.length() == 0) { throw new
	 * RuntimeException("Bad data:" + record); } if(kstr.equals(key)) { value =
	 * vstr; break; } } return value; }
	 */

	public static Row getRecordsByKeysFromFile(String fileName,
			Collection<String> keys, Long offset) {
		Row row = new Row();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			reader.skip(offset);
			String record = reader.readLine();

			String[] kvs = record.split("\t");

			for (int i = 0; i < kvs.length; i++) {
				String str = new String(kvs[i]);
				int p = str.indexOf(":");
				String kstr = str.substring(0, p);
				String vstr = str.substring(p + 1);
				if (kstr.length() == 0 || vstr.length() == 0) {
					throw new RuntimeException("Bad data:" + record);
				}
				if (keys == null || kstr.equals(RaceConfig.orderId)
						|| kstr.equals(RaceConfig.buyerId)
						|| kstr.equals(RaceConfig.goodId)
						|| keys.contains(kstr)) {
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
	 * 从文件里一次读取一行数据出来 最主要的瓶颈
	 * 
	 * @param fileName
	 * @param offset
	 * @return
	 */
	public static String getStringFromFile(
			LinkedBlockingQueue<RandomAccessFile> file, Long offset,
			TableName tableType) {
		//StringBuilder result = null;

		try {
			RandomAccessFile fileReader = file.take();
			fileReader.seek(offset);
			
			byte[] combineData = new byte[0];
			//StringBuilder builder = new StringBuilder();

			byte[] data = new byte[1024];
			int locateEnd = 0;
			boolean isEnd = true;
			while (true) {
//				if (buffer.remaining() < data.length) {
//					data = new byte[buffer.remaining()];
//				}
				fileReader.read(data);
				for (int i = 0; i < data.length; i++) {
					if (data[i] == '\n') {
						isEnd = false;
						locateEnd = i;
						break;
					}
				}
				if (isEnd == false) {
					break;
				}
				// 将这1024个字节合并
//				byte[] newByte = new byte[combineData.length+ data.length];  
//		        System.arraycopy(combineData, 0, newByte, 0, combineData.length);  
//		        System.arraycopy(data, 0, newByte, combineData.length, data.length);
		        combineData = byteMerger(combineData, data);
			}
			// 将最后一撮合并
			byte[] finalByte = new byte[combineData.length+ data.length];  
	        System.arraycopy(combineData, 0, finalByte, 0, combineData.length);  
	        System.arraycopy(data, 0, finalByte, combineData.length, locateEnd);
	        
	        file.add(fileReader);				// 放回
	        return new String(finalByte);
//			result = new StringBuilder();
//			byte[] data = new byte[1024];
//			int locateEnd = 0;
//			boolean isEnd = true;
//			while (true) {
////				if (fileReader.() < data.length) {
////					data = new byte[buffer.remaining()];
////				}
//				fileReader.read(data);
//				for (int i = 0; i < data.length; i++) {
//					if (data[i] == '\n') {
//						isEnd = false;
//						locateEnd = i;
//						break;
//					}
//				}
//				if (isEnd == false) {
//					break;
//				}
//				result.append(new String(data));
//			}
//			result.append(new String(data,0,locateEnd));
			//System.out.println("read from disk:" + result.toString());
//			result = new String(fileReader.readLine().getBytes(
//					StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
			// 放回文件句柄队列中
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;

	}

	/**
	 * 从文件里取出相应的orderid列表byte[]
	 * 
	 * @param fileName
	 * @param offset
	 * @return
	 */
	public static List<byte[]> getOrderIdListsFromFile(
			LinkedBlockingQueue<RandomAccessFile> file, long offset) {
		List<byte[]> results = null;

		try {
			RandomAccessFile fileReader = file.take();
			fileReader.seek(offset);

			byte[] content = new byte[fileReader.readInt()];
			fileReader.read(content);
			results = ByteUtils.splitBytes(content);

			// 放回文件句柄队列中
			file.add(fileReader);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return results;

	}

	/**
	 * 将文件地址下标+offset编码为byte数组
	 * 
	 * @param dataSerialNumber
	 * @param offset
	 * @return
	 */
	public static byte[] encodeIndex(int dataSerialNumber, long offset) {
		byte[] buffer = new byte[RaceConfig.compressed_min_bytes_length];
		buffer[0] = ByteUtils.getMagicByteFromInt(dataSerialNumber);
		byte[] intByte = ByteUtils.leIntToByteArray(ByteUtils
				.getUnsignedInt(offset));
		System.arraycopy(intByte, 0, buffer, RaceConfig.byte_size,
				intByte.length);
		/*
		 * ByteBuffer buffer =
		 * ByteBuffer.allocate(RaceConfig.compressed_min_bytes_length);
		 * //放入源数据文件编号
		 * buffer.put(ByteUtils.getMagicByteFromInt(dataSerialNumber));
		 * buffer.putInt(ByteUtils.getUnsignedInt(offset)); buffer.clear();
		 */
		return buffer;
	}

	/**
	 * 解析indexBytes为文件下标+offset
	 * 
	 * @param dataFileMapping
	 * @param indexBytes
	 */
	/*
	 * public static FileIndexWithOffset decodeIndex(byte[] indexBytes){
	 * ByteBuffer buffer = ByteBuffer.wrap(indexBytes); return new
	 * FileIndexWithOffset( buffer.get(),
	 * ByteUtils.getLongOffset(buffer.getInt())); }
	 */

	/**
	 * 得到一行数据里某个key的value 用于good和buyer表
	 * 
	 * @param line
	 * @return
	 */
	public static String getValueFromLineWithKeyList(String line,
			String targetKey, HashSet<String> keyList) {
		String result = null;
		if (line != null) {
			// int keyHashCode = 0;
			// Row kvMap = new Row();
			String[] kvs = line.split("\t");
			for (String rawkv : kvs) {
				int p = rawkv.indexOf(':');
				String key = rawkv.substring(0, p);
				String value = rawkv.substring(p + 1);
				keyList.add(key);
				if (key.equals(targetKey)) {
					// 找到了所需的key
					result = value;
					// keyHashCode = value.hashCode();
				}
				/*
				 * if (key.length() == 0 || value.length() == 0) { throw new
				 * RuntimeException("Bad data:" + line); }
				 */

			}
			return result;
		} else {
			return null;
		}
	}

	/**
	 * 得到一行数据里某个key的value，并且加入key到列表中
	 * 
	 * @param line
	 * @return
	 */
	public static String getValueFromLine(String line, String key) {
		if (line != null) {
			int location = line.indexOf(key);
			int endLocation = line.indexOf("\t", location);
			return line.substring(location + key.length() + 1,
					endLocation != -1 ? endLocation : line.length());
		} else {
			return null;
		}
	}

	/**
	 * 工具类 可从一行数据中解析出KeyValue对
	 * 
	 * @param line
	 * @return
	 */
	public static Row createKVMapFromLine(String line) {
		try{
			if (line != null && !line.equals("")) {
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
			} else {
				return null;
			}
		} catch( Exception e) {
			System.out.println(line);
			return null;
		}
		
	}

	/**
	 * 工具类 可从一行数据中解析出KeyValue对
	 * 
	 * @param line
	 * @return
	 */
	public static Row createSubKVMapFromLine(String line,
			Collection<String> keys) {
		if (line != null && !line.equals("")) {
			Row kvMap = new Row();
			String[] kvs = line.split("\t");

			for (String rawkv : kvs) {
				int p = rawkv.indexOf(':');
				String key = rawkv.substring(0, p);
				String value = rawkv.substring(p + 1);
				if (key.length() == 0 || value.length() == 0) {
					throw new RuntimeException("Bad data:" + line);
				}
				if (keys == null || keys.contains(key)) {
					kvMap.put(key, new KeyValueImpl(key, value));
				}
			}
			return kvMap;
		} else {
			return null;
		}
	}

	/**
	 * 获得一条记录的主键
	 * 
	 * @param record
	 * @return
	 */
	/*
	 * public static String getIdFromRecord(String record){ String[] kvs =
	 * record.split("\t"); String str = new String(kvs[0]); int p =
	 * str.indexOf(":"); String vstr = str.substring(p+1); return vstr; }
	 * 
	 * //从KeyValue中获取Value public static String getValueFromKV(String keyValue){
	 * String[] kvs = keyValue.split(":"); return kvs[1]; }
	 */

	/**
	 * 将对应的direct memory dump到文件里去
	 * 
	 * @param fileName
	 * @param directMemory
	 * @param type
	 */
	public static void writeToFile(String fileName,
			ByteDirectMemory directMemory, int memoryType) {
		try{
			File file = new File(fileName);
			
			FileChannel wChannel = new FileOutputStream(file).getChannel();
			if (memoryType == RaceConfig.buyerMemory) {
				// 从buyer缓冲区里拿
				directMemory.orderBuyerBuffer.position(0);
				wChannel.write(directMemory.orderBuyerBuffer);
				directMemory.clearOneSegment(memoryType);
			} else if (memoryType == RaceConfig.goodMemory) {
				directMemory.orderGoodBuffer.position(0);
				wChannel.write(directMemory.orderGoodBuffer);
				directMemory.clearOneSegment(memoryType);
			} else {
				directMemory.sharedDirectBuffer.position(0);
				wChannel.write(directMemory.sharedDirectBuffer);
				directMemory.clearOneSegment(memoryType);
			}
			wChannel.close();
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
	
	public static byte[] byteMerger(byte[] byte_1, byte[] byte_2){ 
		if (byte_1 == null) {
			return byte_2;
		}
        byte[] byte_3 = new byte[byte_1.length+byte_2.length];  
        System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);  
        System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);  
        return byte_3;  
    }

//	public static void writeToFile(String fileName, ByteDirectMemory directMemory, DirectMemoryType type) {
//		 File file = new File(fileName);
//		 FileChannel wChannel;
//		try {
//			wChannel = new FileOutputStream(file).getChannel();
//			switch( type) {
//			 case BuyerIdSegment:
//				 directMemory.orderBuyerBuffer.position(0);
//				 wChannel.write(directMemory.orderBuyerBuffer);
//				 directMemory.clearOneSegment(type);
//				 break;
//			 case GoodIdSegment:
//				 directMemory.orderGoodBuffer.position(0);
//				 wChannel.write(directMemory.orderGoodBuffer);
//				 directMemory.clearOneSegment(type);
//				 break;
//			 default:
//				 break;
//			 }
//			 wChannel.close();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		 
//	}
}
