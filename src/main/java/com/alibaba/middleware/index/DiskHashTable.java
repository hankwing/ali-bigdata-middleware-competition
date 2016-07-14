package com.alibaba.middleware.index;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.middleware.conf.RaceConfig;

/**
 * 索引元信息 保存桶数、记录数、使用的位数、桶对应的物理地址等信息 缓冲区管理调用的writeBucket是线程安全的
 * 支持：所有String类型的key，Long型的value, 一key可对应多value
 * 
 * @author hankwing
 *
 */
public class DiskHashTable<T> implements Serializable {

	private static final long serialVersionUID = 6020895636934444399L;
	private int usedBits;
	private int bucketNum;
	private long recordNum;
	private String bucketFilePath = null; // save the buckets data
	private String dataFilePath = null;
	// 保存桶数据 但一加载此类时这个Map是空的 当调用查询时才会从物理地址里load进相应的桶数据
	private transient Map<Integer, HashBucket<T>> bucketList = null;

	private transient ByteArrayOutputStream byteArrayOs = null;
	private transient ObjectOutputStream offsetOos = null;
	private transient BufferedOutputStream bufferedFout;
	private transient FileOutputStream fos;

	private transient FileInputStream streamIn;
	private transient ObjectInputStream bucketReader;
	private transient long lastOffset = 0;
	private Map<Integer, Long> bucketAddressList = null; // 桶对应的物理地址
	private Class<?> classType = null;

	public DiskHashTable() {

	}

	/**
	 * 调用此类时 需要设置桶数据的文件地址和数据的文件地址 初始化10个桶
	 * 
	 * @param bucketFilePath
	 * @param dataFilePath
	 * @throws NoSuchAlgorithmException 
	 */
	public DiskHashTable(String bucketFilePath, String dataFilePath, Class<?> classType){
		usedBits = 1;
		bucketNum = 10;
		recordNum = 0;
		this.classType = classType;
		this.bucketFilePath = bucketFilePath;
		this.dataFilePath = dataFilePath;
		bucketList = new ConcurrentHashMap<Integer, HashBucket<T>>();
		bucketAddressList = new ConcurrentHashMap<Integer, Long>();
		for (int i = 9; i >= 0; i--) {
			bucketList.put(i, new HashBucket<T>(this, i, classType));
		}
	}

	/**
	 * 从文件里读取此类时 调用restore恢复初始化一些数据
	 */
	public void restore() {
		bucketList = new HashMap<Integer, HashBucket<T>>();

	}

	/**
	 * 将某个桶写到外存 在Map里保存该桶的物理地址以便之后重新load到内存 线程安全
	 * 
	 * @param bucketKey
	 */
	public synchronized void writeBucket(int bucketKey) {

		try {
			long offset = 0;

			if (byteArrayOs == null || fos == null) {
				byteArrayOs = new ByteArrayOutputStream();
				fos = new FileOutputStream(bucketFilePath, true);

				if (fos.getChannel().position() > 4) {
					// 追加模式
					offsetOos = new AppendingObjectOutputStream(byteArrayOs);
				} else {
					// 第一次打开桶文件 需要写入头数据
					offsetOos = new ObjectOutputStream(byteArrayOs);
				}

				bufferedFout = new BufferedOutputStream(fos);
				offset = fos.getChannel().position();
				bufferedFout.write(byteArrayOs.toByteArray());
				bufferedFout.flush();

			}
			byteArrayOs.reset();

			offset = fos.getChannel().position();
			bucketAddressList.put(bucketKey, offset);

			offsetOos.writeObject(bucketList.remove(bucketKey));
			bufferedFout.write(byteArrayOs.toByteArray());
			bufferedFout.flush();
			offsetOos.reset();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 索引建立完之后 将所有桶数据写到外存 不调用单个写桶的函数 因为会频繁调用flush影响效率
	 */
	public void writeAllBuckets() {

		try {
			if (bufferedFout == null || offsetOos == null) {
				byteArrayOs = new ByteArrayOutputStream();
				
				fos = new FileOutputStream(bucketFilePath, true);
				
				if (fos.getChannel().position() > 4) {
					// 追加模式
					offsetOos = new AppendingObjectOutputStream(byteArrayOs);
				} else {
					// 第一次打开桶文件 需要写入头数据
					offsetOos = new ObjectOutputStream(byteArrayOs);
				}
				
				bufferedFout = new BufferedOutputStream(fos);
				bufferedFout.write(byteArrayOs.toByteArray());
				lastOffset = byteArrayOs.size() + fos.getChannel().position();
				// bucketWriter = new ObjectOutputStream(bufferedFout);

			} else {
				lastOffset = fos.getChannel().position();
			}
			for (Map.Entry<Integer, HashBucket<T>> writeBucket : bucketList
					.entrySet()) {
				bucketAddressList.put(writeBucket.getKey(), lastOffset);
				byteArrayOs.reset();
				offsetOos.writeObject(writeBucket.getValue());
				
				lastOffset += byteArrayOs.size();
				// bucketWriter.writeObject(writeBucket.getValue());
				bufferedFout.write(byteArrayOs.toByteArray());
				offsetOos.reset();

			}
			bufferedFout.flush();
			bufferedFout.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 内部调用函数 读取某个桶到内存中
	 * 
	 * @param bucketKey
	 * @return
	 */
	public HashBucket<T> readBucket(int bucketKey) {

		HashBucket<T> fileBucket = null;
		try {
			FileInputStream streamIn = new FileInputStream(bucketFilePath);
			ObjectInputStream bucketReader = new ObjectInputStream(streamIn);
		    //bucketReader.readObject();			// 必须得读一下  找到类的描述符
			//}
			streamIn.getChannel().position(bucketAddressList.get(bucketKey));

			fileBucket = (HashBucket<T>) bucketReader.readObject();
			bucketList.put(bucketKey, fileBucket);
			
			bucketReader.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return fileBucket;

	}

	/**
	 * 通过key值得到对应数据的地址偏移量
	 * 
	 * @param key
	 * @return
	 */
	public List<Long> get(String key) {

		HashBucket<T> bucket = null;
		int bucketIndex = getBucketIndex(key);
		System.out.println("bucketIndex:" + bucketIndex);
		if (bucketIndex < bucketNum) {
			bucket = bucketList.get((int) bucketIndex);
			if (bucket == null) {
				System.out.println("load bucketIndex:" + bucketIndex);
				bucket = readBucket(bucketIndex);
			}
		} else {
			bucket = bucketList.get((int) (bucketIndex % Math.pow(10,
					usedBits - 1)));
			if (bucket == null) {
				System.out.println("load bucketIndex:" + bucketIndex);
				bucket = readBucket((int) (bucketIndex % Math.pow(10,
						usedBits - 1)));
			}
		}

		if (bucket != null) {
			return bucket.getAddress(getBucketStringIndex(key), key);
		} else {
			// need to read from file
			System.out.println("read error!");
			return null;
		}

	}

	/**
	 * put key & address
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean put(String key, long value) {

		HashBucket<T> bucket = null;
		int bucketIndex = getBucketIndex(key);
		if (bucketIndex < bucketNum) {
			bucket = bucketList.get((int) bucketIndex);
			if (bucket == null) {
				bucket = readBucket(bucketIndex);
			}
		} else {
			bucket = bucketList.get((int) (bucketIndex % Math.pow(10,
					usedBits - 1)));
			if (bucket == null) {
				bucket = readBucket((int) (bucketIndex % Math.pow(10,
						usedBits - 1)));
			}

		}

		if (bucket != null) {
			bucket.putAddress(getBucketStringIndex(key), key, value);
			if (++recordNum / bucketNum > RaceConfig.hash_index_block_capacity * 0.8) {
				// 增加新桶
				HashBucket<T> newBucket = new HashBucket<T>(this, bucketNum, classType);
				bucketNum++;
				bucketList.put(bucketNum - 1, newBucket);
				if (bucketNum > Math.pow(10, usedBits)) {
					usedBits++;
				}

				int newBucketIndex = bucketNum - 1;
				HashBucket<T> modifyBucket = bucketList
						.get((int) (newBucketIndex % Math.pow(10, usedBits - 1)));
				List<Map<String, T>> temp = 
						modifyBucket.getAllValues(String.valueOf(newBucketIndex));
				for (Map<String, T> tempMap : temp) {

					for (Iterator<Map.Entry<String, T>> it = tempMap
							.entrySet().iterator(); it.hasNext();) {
						Map.Entry<String, T> entry = it.next();
						if (getBucketIndex(entry.getKey()) == newBucketIndex) {
							newBucket.putAddress(getBucketStringIndex(entry.getKey()),entry.getKey(),
									entry.getValue());
							if( entry.getValue().getClass() == List.class) {
								modifyBucket.minusRecordNum( ((List<Long>)entry.getValue()).size());
							}
							else {
								modifyBucket.minusRecordNum(1);
							}
							
							it.remove();
						}
					}
				}

			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 通过key值算桶号
	 * 
	 * @param key
	 * @return
	 */
	public int getBucketIndex(String key) {

		int bucketIndex = Math.abs(key.hashCode());
		if( bucketIndex < Math.pow(10, usedBits)) {
			return bucketIndex;
		}
		else {
			return (int) (bucketIndex % Math.pow(10, usedBits));
		}
	}
	
	/**
	 * 
	 * 
	 * @param key
	 * @return
	 */
	public String getBucketStringIndex(String key) {
		key = String.valueOf(Math.abs(key.hashCode()));
		return key;

	}

	/**
	 * 索引建立、写出完毕后 调用此函数释放文件句柄
	 */
	public void cleanup() {
		try {
			if (fos != null) {
				fos.close();
			}
			if (bufferedFout != null) {
				bufferedFout.close();
			}
			if (streamIn != null) {
				streamIn.close();
			}
			if (bucketReader != null) {
				bucketReader.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
