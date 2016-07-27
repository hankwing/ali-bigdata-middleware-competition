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
	private String bucketFilePath = null; // save the buckets data
	//private String dataFilePath = null;
	// 保存桶数据 但一加载此类时这个Map是空的 当调用查询时才会从物理地址里load进相应的桶数据
	private transient Map<Integer, HashBucket<K,T>> bucketList = null;

	private transient ByteArrayOutputStream byteArrayOs = null;
	private transient ObjectOutputStream offsetOos = null;
	private transient BufferedOutputStream bufferedFout;
	private transient FileOutputStream fos;
	
	private transient long lastOffset = 0;
	private transient ReadWriteLock readWriteLock = null;
	private transient BucketCachePool bucketCachePool = null;
	private transient LinkedBlockingQueue<BucketReader>
		bucketReaderPool = null;
	private long bucketAddressOffset = 0;					// 存桶对应物理地址的map的offset
	private Map<Integer, Long> bucketAddressList = null; // 桶对应的物理地址
	private Class<?> classType = null;
	
	
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
		bucketList = new HashMap<Integer, HashBucket<K,T>>();
		readWriteLock = new ReentrantReadWriteLock();
		bucketCachePool = BucketCachePool.getInstance();
	}

	/**
	 * 将某个桶写到外存 在Map里保存该桶的物理地址以便之后重新load到内存 线程安全
	 * 
	 * @param bucketKey
	 */
	public void writeBucket(int bucketKey) {
		// 直接从队列里remove桶
		bucketList.remove(bucketKey);
		/*try {
			
			long offset = 0;
			
			if (byteArrayOs == null || bufferedFout == null) {
				byteArrayOs = new ByteArrayOutputStream();
				fos = new FileOutputStream(bucketFilePath, true);

				if (fos.getChannel().position() > 4) {
					// 追加模式
					offsetOos = new AppendingObjectOutputStream(byteArrayOs);
				} else {
					// 第一次打开桶文件 需要写入头数据
					offsetOos = new ObjectOutputStream(byteArrayOs);
				}
				readWriteLock.writeLock().lock();					// 加写锁
				bufferedFout = new BufferedOutputStream(fos);
				offset = fos.getChannel().position();
				bufferedFout.write(byteArrayOs.toByteArray());
				bufferedFout.flush();
				readWriteLock.writeLock().unlock();					// 解写锁

			}
			
			readWriteLock.writeLock().lock();						// 加写锁
			byteArrayOs.reset();
			
			offset = fos.getChannel().position();
			bucketAddressList.put(bucketKey, offset);
			offsetOos.writeObject(bucketList.remove(bucketKey));
			
			bufferedFout.write(byteArrayOs.toByteArray());
			bufferedFout.flush();
			offsetOos.reset();
			readWriteLock.writeLock().unlock();						// 解写锁
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

	}

	/**
	 * 索引建立完之后 将所有桶数据写到外存 不调用单个写桶的函数 因为会频繁调用flush影响效率 
	 * 返回值：此DiskHashTable被写入dataFile的哪个位置，方便之后调用
	 */
	public void writeAllBuckets() {
		long thisOffset = 0;
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
			for (Map.Entry<Integer, HashBucket<K,T>> writeBucket : bucketList
					.entrySet()) {
				bucketAddressList.put(writeBucket.getKey(), lastOffset);
				byteArrayOs.reset();
				//offsetOos = new ObjectOutputStream(byteArrayOs);
				offsetOos.writeUnshared(writeBucket.getValue());
				offsetOos.reset();
				lastOffset += byteArrayOs.size();
				// bucketWriter.writeObject(writeBucket.getValue());
				bufferedFout.write(byteArrayOs.toByteArray());
				
			}
			
			// write this HashTable to dataFile and return offset
			bucketList = new ConcurrentHashMap<Integer, HashBucket<K,T>>();		// 清空map
			// 建立索引文件句柄缓冲池
			for( int i =0; i < RaceConfig.fileHandleNumber; i++) {
				FileInputStream streamIn = new FileInputStream(bucketFilePath);
				ObjectInputStream bucketReader = new ObjectInputStream(streamIn);
				bucketReaderPool.add(new BucketReader(streamIn, bucketReader));
			}
			// 把桶对应物理地址的map写出去  减少内存开销
			byteArrayOs.reset();
			ObjectOutputStream oos = new ObjectOutputStream(byteArrayOs);
			bucketAddressOffset = lastOffset ;
			oos.writeObject(bucketAddressList);
			bucketAddressList = null;				// 清空桶的地址列表数据  空出内存
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
	 * 内部调用函数 读取某个桶到内存中
	 * 
	 * @param bucketKey
	 * @return
	 */
	public HashBucket<K,T> readBucket(int bucketKey) {
		HashBucket<K,T> fileBucket = bucketList.get( bucketKey);
		try {
			if( fileBucket == null) {
				// 需要从文件里读桶 该桶需要缓冲区管理
				readWriteLock.readLock().lock();
				//if( streamIn == null) {
				if(bucketAddressList == null) {
					// 需要从文件中读出该map
					bucketAddressList = getHashDiskTable(bucketAddressOffset);
				}
				
				//byte[] bucketByteArray = new byte[20000];
				FileInputStream streamIn = new FileInputStream(bucketFilePath);
				//streamIn.getChannel().position(bucketAddressList.get(bucketKey));
				//streamIn.read(bucketByteArray);
				//ByteArrayInputStream bais = new ByteArrayInputStream(bucketByteArray);
				ObjectInputStream bucketReader = new ObjectInputStream(streamIn);
				streamIn.getChannel().position(bucketAddressList.get(bucketKey));
				//}
				//BucketReader reader = bucketReaderPool.take();
				//}
				
				//streamIn.getChannel().position(bucketAddressList.get(bucketKey));
				fileBucket = (HashBucket<K,T>) bucketReader.readObject();
				//bucketReader.close();
				//reader.bucketReader.readObject();
				//if( reader.bucketReader.available() > 0) {
				//	//reader.bucketReader.readByte();
				//}
				
				//reader.bucketReader.readObject();
				//reader.bucketReader.readObject();
				// 缓冲一定数量的桶到内存
				/*for( int i= bucketKey + 1; i < RaceConfig.bucketNumberOneRead && i < bucketNum ; i++) {
					HashBucket<K,T> cacheBucket = (HashBucket<K,T>) bucketReader.readObject();
					cacheBucket.setContext(this);
					bucketList.put(bucketKey, cacheBucket);
					bucketCachePool.addBucket(fileBucket);			// 放入缓冲区
				}
				readWriteLock.readLock().unlock();*/
				bucketReader.close();
				fileBucket.setContext(this);
				//System.out.println("load bucket:" + bucketKey);
				bucketList.put(bucketKey, fileBucket);
				bucketCachePool.addBucket(fileBucket);			// 放入缓冲区
				readWriteLock.readLock().unlock();

				
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

}
