package com.alibaba.middleware.index;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.conf.RaceConfig;
import com.sun.org.apache.bcel.internal.generic.NEW;
import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;

/**
 * 索引元信息 保存桶数、记录数、使用的位数、桶对应的物理地址等信息 缓冲区管理调用的writeBucket是线程安全的
 * 支持：所有String类型的key，Long型的value, 一key可对应多value
 * 
 * @author hankwing
 *
 */
public class DiskHashTable<K,T> implements Serializable {

	private static final long serialVersionUID = 6020895636934444399L;
	private int usedBits;
	private int bucketNum;
	private long recordNum;
	// 保存数据的外村文件路径
	private String bucketFilePath = null; 

	// 保存桶数据 但一加载此类时这个Map是空的 当调用查询时才会从物理地址里load进相应的桶数据
	private transient Map<Integer, HashBucket<K,T>> bucketList = null;

	private transient ByteArrayOutputStream byteArrayOs = null;
	private transient ObjectOutputStream offsetOos = null;
	private transient BufferedOutputStream bufferedFout = null;
	private transient FileOutputStream fos;

	private transient long lastOffset = 0;
	private transient ReadWriteLock readWriteLock = null;
	private transient BucketCachePool bucketCachePool = null;
	private transient LinkedBlockingQueue<BucketReader> bucketReaderPool = null;
	private long bucketAddressOffset = 0;					// 存桶对应物理地址的map的offset


	/**
	 * 查询时bucketAddressList作为直接内存的偏移地址存储链表
	 */
	private Map<Integer, Long> bucketPositionList = null;
	/**
	 * 查询时bucketAddressList作为外存文件的偏移地址存储链表
	 */
	private Map<Integer, Long> bucketAddressList = null; // 桶对应的物理地址

	private Class<?> classType = null;

	public static boolean isbuilding = true;
	//private transient LinkedBlockingQueue<HashBucket<T>> bucketQueue = null;
	//private transient Timer timer  = null;

	public DiskHashTable() {

	}

	/**
	 * 调用此类时 需要设置桶数据的文件地址和数据的文件地址 初始化10个桶
	 * 
	 * @param bucketFilePath
	 * @param dataFilePath
	 * @throws NoSuchAlgorithmException 
	 */
	public DiskHashTable(String bucketFilePath, Class<?> classType){
		usedBits = 1;
		bucketNum = 10;
		recordNum = 0;
		readWriteLock = new ReentrantReadWriteLock();
		this.classType = classType;
		this.bucketFilePath = bucketFilePath;
		bucketList = new ConcurrentHashMap<Integer, HashBucket<K,T>>();

		bucketAddressList = new ConcurrentHashMap<Integer, Long>();
		bucketPositionList = new ConcurrentHashMap<Integer, Long>();

		bucketCachePool = BucketCachePool.getInstance();
		bucketReaderPool = new LinkedBlockingQueue<BucketReader>();
		for (int i = 0; i < 10; i++) {
			HashBucket<K,T> newBucket = new HashBucket<K,T>(this, i, classType);
			bucketList.put(i, newBucket );
		}
	}

	/**
	 * 从文件里读取此类时 调用restore恢复初始化一些数据
	 */
	public void restore() {
		bucketList = new ConcurrentHashMap<Integer, HashBucket<K,T>>();
		readWriteLock = new ReentrantReadWriteLock();
		bucketCachePool = BucketCachePool.getInstance();
	}

	/**
	 * 索引建立完之后 将所有桶数据写到外存 不调用单个写桶的函数 因为会频繁调用flush影响效率 
	 * 返回值：此DiskHashTable被写入dataFile的哪个位置，方便之后调用
	 */
	public void writeAllBuckets() {
		try {
			//timer.cancel();
			readWriteLock.writeLock().lock();
			if (bufferedFout == null || offsetOos == null) {
				byteArrayOs = new ByteArrayOutputStream();

				fos = new FileOutputStream(bucketFilePath);
				offsetOos = new ObjectOutputStream(byteArrayOs);

				bufferedFout = new BufferedOutputStream(fos);
				bufferedFout.write(byteArrayOs.toByteArray());
				lastOffset = byteArrayOs.size() + fos.getChannel().position();
				// bucketWriter = new ObjectOutputStream(bufferedFout);

			} else {
				lastOffset = fos.getChannel().position();
			}

			//遍历bucketPositionList，将直接内存bucket写出
			for (Map.Entry<Integer, Long> bucketAddress : bucketPositionList.entrySet()){
				HashBucket<K, T> bucket = readBucket(bucketAddress.getKey());
				bucketAddressList.put(bucket.getBucketKey(), lastOffset);
				byteArrayOs.reset();

				offsetOos.writeUnshared(bucket);
				offsetOos.reset();
				lastOffset += byteArrayOs.size();
				bufferedFout.write(byteArrayOs.toByteArray());
			}

			for (Map.Entry<Integer, HashBucket<K,T>> writeBucket : bucketList
					.entrySet()) {
				bucketAddressList.put(writeBucket.getKey(), lastOffset);
				byteArrayOs.reset();

				offsetOos.writeUnshared(writeBucket.getValue());
				offsetOos.reset();
				lastOffset += byteArrayOs.size();

				bufferedFout.write(byteArrayOs.toByteArray());
			}

			// write this HashTable to dataFile and return offset
			bucketList = new ConcurrentHashMap<Integer, HashBucket<K,T>>();		// 清空map

			// 建立索引文件句柄缓冲池
			/*for( int i =0; i < RaceConfig.fileHandleNumber; i++) {
				FileInputStream streamIn = new FileInputStream(bucketFilePath);
				ObjectInputStream bucketReader = new ObjectInputStream(streamIn);
				bucketReaderPool.add(new BucketReader(streamIn, bucketReader));
			}*/
			// 把桶对应物理地址的map写出去  减少内存开销
			byteArrayOs.reset();
			ObjectOutputStream oos = new ObjectOutputStream(byteArrayOs);
			bucketAddressOffset = lastOffset ;
			oos.writeObject(bucketAddressList);
			bucketAddressList = null;				// 清空桶的地址列表数据  空出内存
			bucketPositionList = null;
			oos.flush();
			bufferedFout.write(byteArrayOs.toByteArray());
			oos.close();
			bufferedFout.flush();
			//bufferedFout.close();
			readWriteLock.writeLock().unlock();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 根据偏移量读取桶对应物理地址的偏移量
	 * 
	 * @param filePath
	 * @param offSet
	 * @return
	 */
	public Map<Integer, Long> getHashDiskTable(long offSet) {
		Map<Integer, Long> bucketAddressList = null;
		FileInputStream streamIn;
		try {
			streamIn = new FileInputStream(bucketFilePath);
			streamIn.getChannel().position(offSet);
			ObjectInputStream objectinputstream = new ObjectInputStream(
					streamIn);

			bucketAddressList = (Map<Integer, Long>) objectinputstream.readObject();
			objectinputstream.close();
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
		return bucketAddressList;

	}

	/**
	 * 将某个桶写到直接内存在bucketAddreeeList中保存桶的偏移地址以便之后重新load到内存中
	 * @param bucketKey
	 */
	public void writeBucket(int bucketKey) {
		try {			
			//第一次写入到直接内存时，创建byteArrayOs。
			if (byteArrayOs == null) {
				byteArrayOs = new ByteArrayOutputStream();
				offsetOos = new ObjectOutputStream(byteArrayOs);
			} 

			lastOffset = ByteDirectMemory.getPosition();
			//bucketPositionList存储bucket的key和offset
			bucketPositionList.put(bucketKey, lastOffset);

			//写入对象流，传入字节流
			HashBucket<K, T> bucket = bucketList.remove(bucketKey);
			offsetOos.writeObject(bucket);

			//写入直接内存
			ByteDirectMemory.put(byteArrayOs.toByteArray());

			offsetOos.flush();
			byteArrayOs.flush();

		} catch (Exception e) {
			System.out.println(e);
		}
	}

	/**
	 * 内部调用函数，从直接内存读取某个桶到内存中
	 * 查询阶段通过mappedfile进行加载数据
	 * 
	 * @param bucketKey
	 * @return
	 */
	public HashBucket<K,T> readBucket(int bucketKey) {
		HashBucket<K,T> fileBucket = bucketList.get( bucketKey);
		try {
			if( fileBucket == null) {

				if (isbuilding) {
					//从直接内存拿数据
					Long startp = bucketPositionList.get(bucketKey);
					Long endp = null;
					if (bucketPositionList.get(bucketKey+1) != null) {
						endp = bucketPositionList.get(bucketKey+1);
					}else {
						endp = (long) ByteDirectMemory.getPosition();
					}
					byte[] bucketbytes;
					synchronized (ByteDirectMemory.buffer) {
						bucketbytes = ByteDirectMemory.get(startp.intValue(), endp.intValue() - startp.intValue());
					}
					ByteArrayInputStream streamIn = new ByteArrayInputStream(bucketbytes);
					ObjectInputStream bucketReader = new ObjectInputStream(streamIn);

					fileBucket = (HashBucket<K,T>) bucketReader.readObject();
					fileBucket.setContext(this);
					bucketReader.close();
					bucketPositionList.remove(bucketKey);

				} else {
					//从文件中读取数据
					
					FileInputStream streamIn = new FileInputStream(bucketFilePath);
					ObjectInputStream bucketReader = new ObjectInputStream(streamIn);
					if(bucketAddressList == null) {
						// 需要从文件中读出该map
						bucketAddressList = getHashDiskTable(bucketAddressOffset);
					}
					streamIn.getChannel().position(bucketAddressList.get(bucketKey));
					fileBucket = (HashBucket<K,T>) bucketReader.readObject();
					bucketReader.close();
					fileBucket.setContext(this);

				}

				bucketList.put(bucketKey, fileBucket);
				bucketCachePool.addBucket(fileBucket);
				
			}

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
	public List<byte[]> get(K key) {

		HashBucket<K,T> bucket = null;
		int bucketIndex = getBucketIndex( key);
		if (bucketIndex < bucketNum) {
			bucket = readBucket((int) bucketIndex);
		} else {
			bucket = readBucket((int) (bucketIndex % Math.pow(10,
					usedBits - 1)));
		}
		if (bucket != null) {
			return bucket.getAddress(getBucketStringIndex( key), key);
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
	public boolean put(K key, byte[] value) {

		HashBucket<K,T> bucket = null;
		int bucketIndex = getBucketIndex(key);
		if (bucketIndex < bucketNum) {
			bucket = readBucket((int) bucketIndex);
		} else {
			bucket = readBucket((int) (bucketIndex % Math.pow(10,
					usedBits - 1)));

		}

		if (bucket != null) {			
			bucket.putAddress(getBucketStringIndex(key), key, value);
			if (++recordNum / bucketNum > RaceConfig.hash_index_block_capacity * 0.8) {
				// 增加新桶
				HashBucket<K,T> newBucket = new HashBucket<K,T>(this, bucketNum, classType);
				//BucketCachePool.getInstance().addBucket(newBucket);
				bucketNum++;
				bucketList.put(bucketNum - 1, newBucket);

				if (bucketNum > Math.pow(10, usedBits)) {
					usedBits++;
				}

				int newBucketIndex = bucketNum - 1;
				HashBucket<K,T> modifyBucket = readBucket(
						(int) (newBucketIndex % Math.pow(10, usedBits - 1)));

				List<Map<K, T>> temp = 
						modifyBucket.getAllValues(String.valueOf(newBucketIndex));
				for (Map<K, T> tempMap : temp) {

					for (Iterator<Map.Entry<K, T>> it = tempMap
							.entrySet().iterator(); it.hasNext();) {
						Map.Entry<K, T> entry = it.next();
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
	public int getBucketIndex(K key) {

		int bucketIndex = Math.abs(key.hashCode());
		double temp = Math.pow(10, usedBits);
		if( bucketIndex < temp) {
			return bucketIndex;
		}
		else {
			return (int) (bucketIndex % temp);
		}
	}

	/**
	 * 
	 * 
	 * @param key
	 * @return
	 */
	public String getBucketStringIndex(K key) {
		String stringKey = String.valueOf(Math.abs(key.hashCode()));
		return stringKey;

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
			/*if (bucketReader != null) {
				bucketReader.close();
			}*/
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setIsbuilding(boolean isbuilding) {
		this.isbuilding = isbuilding;
	}

	public static void main(String args[]){
		ByteDirectMemory directMemory = new ByteDirectMemory(Integer.MAX_VALUE);
		DiskHashTable<Integer, List<byte[]>> table = new DiskHashTable<Integer,List<byte[]>>("1.txt",List.class);
		table.put(1, "wxl".getBytes());
		table.writeBucket(1);
		List<byte[]> list = table.get(1);
		for (int i = 0; i < list.size(); i++) {
			System.out.println(new String(list.get(i)));
		}
	}

}
