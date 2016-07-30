package com.alibaba.middleware.index;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.tools.RecordsUtils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.LZFException;

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
	public long recordNum;
	public long memRecordNum;					// 内存里保留的记录条数  根据这个指标判断是否将一些桶写入direct memory
	// 保存数据的外村文件路径
	private String bucketFilePath = null; 
	private ReentrantReadWriteLock readWriteLock = null;

	// 保存桶数据 但一加载此类时这个Map是空的 当调用查询时才会从物理地址里load进相应的桶数据
	public transient Map<Integer, HashBucket<K,T>> bucketList = null;
	//private transient ByteArrayOutputStream byteArrayOs = null;
	//private transient ObjectOutputStream offsetOos = null;
	private transient BufferedOutputStream bufferedFout = null;
	private transient FileOutputStream fos;
	private transient long lastOffset = 0;
	private transient BucketCachePool bucketCachePool = null;			// 每建一个桶就往里注册一个

	private transient LinkedBlockingQueue<RandomAccessFile>
	bucketReaderPool = null;
	private transient ByteDirectMemory directMemory = null;
	private long bucketAddressOffset = 0;					// 存桶对应物理地址的map的offset
	private int lastObjectSize = 0;
	//private Registration classReg = null;					// kryo用的注册类
	/**
	 * 查询时bucketAddressList作为外存文件的偏移地址存储链表
	 */
	private Map<Integer, Long> bucketAddressList = null; // 桶对应的物理地址 在创建的时候也用这个Map
	private Map<Integer, Long> bucketDirectMemList = null; // 桶对应的直接内存地址  在查询阶段内存里的桶优先写到直接内存

	private Class<?> classType = null;

	public boolean isbuilding = false;						// 先不测这个
	private DirectMemoryType memoryType = null;
	private KryoContext kryoContext = null;
	//private FIFOCache bucketWriterWhenBuilding = null;

	//private transient LinkedBlockingQueue<HashBucket<K,T>> bucketQueue = null;
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
	public DiskHashTable( String bucketFilePath, 
			Class<?> classType, DirectMemoryType memoryType){
		this.memoryType = memoryType;
		usedBits = 1;
		bucketNum = 10;
		recordNum = 0;
		memRecordNum = 0;
		this.classType = classType;
		this.bucketFilePath = bucketFilePath;
		bucketList = new ConcurrentHashMap<Integer, HashBucket<K,T>>();
		// 在调用writeAllBucket的时候禁止读写
		readWriteLock = new ReentrantReadWriteLock();
		bucketAddressList = new ConcurrentHashMap<Integer, Long>();
		bucketDirectMemList = new ConcurrentHashMap<Integer,Long>();
		directMemory = ByteDirectMemory.getInstance();			//	获取direct memory
		//directMemory.clear();									// 先不清空了
		bucketCachePool = BucketCachePool.getInstance();
		bucketReaderPool = new LinkedBlockingQueue<RandomAccessFile>();
		// 注册将桶写到direct memory的监控线程
		//bucketWriterWhenBuilding = new FIFOCache(this);
		//FIFOCacheMonitorThread.getInstance().registerFIFIOCache(bucketWriterWhenBuilding);
		
		kryoContext = KryoContext.newKryoContextFactory(new KryoClassRegistrator(){
		    @Override
		    public void register(Kryo kryo) {
		        kryo.register(HashBucket.class);  
		    }       
		});
		
		for (int i = 0; i < 10; i++) {
			HashBucket<K,T> newBucket = new HashBucket<K,T>(this, i, classType);
			//bucketCachePool.addBucket(newBucket);
			//bucketWriterWhenBuilding.addBucket(newBucket);
			bucketList.put(i, newBucket );
		}
	}

	/**
	 * 从文件里读取此类时 调用restore恢复初始化一些数据
	 */
	public void restore() {
		bucketList = new ConcurrentHashMap<Integer, HashBucket<K,T>>();
		//bucketCachePool = BucketCachePool.getInstance();
	}

	/**
	 * 索引建立完之后 将所有桶数据写到外存 不调用单个写桶的函数 因为会频繁调用flush影响效率 
	 * 返回值：此DiskHashTable被写入dataFile的哪个位置，方便之后调用
	 */
	public void writeAllBuckets() {
		try {
			// 往kryo里注册类
			if (bufferedFout == null) {
				//byteArrayOs = new ByteArrayOutputStream();
				//output = new Output( byteArrayOs);
				fos = new FileOutputStream(bucketFilePath);
				//offsetOos = new ObjectOutputStream(byteArrayOs);
				bufferedFout = new BufferedOutputStream(fos);
				lastOffset = fos.getChannel().position();
			}
			
			for (int key = 0; key < bucketNum ; key ++) {
				// 优先把桶写到直接内存里  满了就写到文件里
				if( memoryType != DirectMemoryType.NoWrite && writeBucketAfterBuilding(key) ) {
					// 如果是订单表的三个索引  那么先写到直接内存
					//System.out.println("write to direct memory success:" + key);
				}
				else {
					// 否则写到文件里
					//System.out.println("write to file:" + key);
					HashBucket<K,T> writeBucket = readBucket(key);
					if( writeBucket == null) {
						// error
						System.out.println("cannot find bucket !");
						System.exit(0);
					}
					bucketAddressList.put(key, lastOffset);
					byte[] objectByte = kryoContext.serialze(writeBucket);
					lastOffset += objectByte.length;
					lastObjectSize = objectByte.length;		// 存最后一个桶的size
					bufferedFout.write(objectByte);
					bucketList.remove(key);					// 及时移走
				}
				
			}

			//buffer output stream flush to file
			bufferedFout.flush();
			// write this HashTable to dataFile and return offset
			bucketList = new ConcurrentHashMap<Integer, HashBucket<K,T>>();		// 清空map
			//directMemory.clear();												// 清空直接内存
			// 建立索引文件句柄缓冲池
			for( int i =0; i < RaceConfig.fileHandleNumber; i++) {
				RandomAccessFile streamIn = new RandomAccessFile(bucketFilePath,"r");
				//ObjectInputStream bucketReader = new ObjectInputStream(streamIn);
				bucketReaderPool.add(streamIn);
			}
			// 把桶对应物理地址的map写出去  减少内存开销
			bucketAddressOffset = lastOffset;			
			byte[] bucketAddressByte = kryoContext.serialze(bucketAddressList);// 桶地址列表的地址
			bucketAddressList = null;				// 清空桶的地址列表数据  空出内存
			bufferedFout.write(bucketAddressByte);
			
			bufferedFout.flush();
			isbuilding = false;
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
	@SuppressWarnings("unchecked")
	public Map<Integer, Long> getHashDiskTable(long offSet) {
		Map<Integer, Long> bucketAddressList = null;
		FileInputStream streamIn;
		try {
			streamIn = new FileInputStream(bucketFilePath);
			streamIn.getChannel().position(offSet);
			Input input = new Input(streamIn);
			Kryo kryo = new Kryo();
			//ObjectInputStream bucketReader = new ObjectInputStream(
			//		new ByteArrayInputStream(bucketbytes));
			
			bucketAddressList = (ConcurrentHashMap<Integer, Long>) 
					kryo.readObject(input, ConcurrentHashMap.class);
			input.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return bucketAddressList;

	}

	/**
	 * 将某个桶写到直接内存在bucketAddreeeList中保存桶的偏移地址以便之后重新load到内存中
	 * @param bucketKey
	 */
	public void writeBucketWhenBuilding(int bucketKey) {
		try {			
			if(isbuilding) {
				// 只能在building的时候调用这个方法
				System.out.println("write bucket to direct mem:" + bucketKey);
				//如果byteArrayOs为空，则创建byteArrayOs
				
				/*if (byteArrayOs == null) {
					byteArrayOs = new ByteArrayOutputStream();
				}
				//Resets the count field of this byte array output stream to zero
				byteArrayOs.reset();*/
				
				/*//创建对象输出流
				offsetOos = new ObjectOutputStream(byteArrayOs);
				
				//加锁
				readWriteLock.writeLock().lock();
				offsetOos.writeObject(bucketList.remove(bucketKey));
				int newPos = directMemory.put(byteArrayOs.toByteArray(), memoryType);
				if( newPos != -1 ) {
					//如果写入成功
					bucketAddressList.put(bucketKey, (long) newPos);
					
				}
				else {
					System.out.println("direct memory is full");
				}
				offsetOos.reset();
				readWriteLock.writeLock().unlock();*/
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 查询阶段调用的方法  将桶优先写入直接内存  直接内存无空间了就直接丢弃
	 * @param bucketKey
	 */
	public boolean writeBucketAfterBuilding(int bucketKey) {
		
		//bucketList.remove(bucketKey);
		boolean isSuccess = false;
		HashBucket<K, T> bucketToRemove = bucketList.get(bucketKey);
		if( !directMemory.isFull(memoryType) ) {
			// 有空间  试图往里写 但不一定写成功

			byte[] objectByte = kryoContext.serialze(bucketToRemove);
			int newPos = directMemory.put(objectByte, memoryType);
			if( newPos != -1 ) {
				//如果写入成功 则放入另一个地址队列 不能覆盖文件的物理地址队列
				isSuccess = true;
				bucketList.remove(bucketKey);
				bucketDirectMemList.put(bucketKey, (long) newPos);
			}
			else {
				
				System.out.println("direct memory is full");
			}
		}
		
		return isSuccess;
		
		
	}
	
	/**
	 * 直接丢弃
	 * @param bucketKey
	 */
	public void discardBucket(int bucketKey) {
		//System.out.println("discard bucket");
		bucketList.remove(bucketKey);
	}
	
	/**
	 * 内部调用函数，从直接内存读取某个桶到内存中
	 * 查询阶段通过mappedfile进行加载数据
	 * 
	 * @param bucketKey
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public HashBucket<K,T> readBucket(int bucketKey) {
		HashBucket<K,T> fileBucket = bucketList.get( bucketKey);
		try {
			if( fileBucket == null) {
				//readWriteLock.readLock().lock();
				if (isbuilding) {
					//从直接内存拿数据
					readWriteLock.readLock().lock();
					//System.out.println("read bucket from direct mem:" + bucketKey);
					long startp = bucketAddressList.get(bucketKey);
					byte[] bucketbytes;

					bucketbytes = directMemory.get((int)startp, memoryType);

					ObjectInputStream bucketReader = new ObjectInputStream(
							new ByteArrayInputStream(bucketbytes));

					fileBucket = (HashBucket<K,T>) bucketReader.readObject();
					fileBucket.setContext(this);
					bucketReader.close();
					readWriteLock.readLock().unlock();

				} else {
					// 查询阶段  先从directMemory拿桶数据  拿不到再从文件中读取数据
					// 下面从direct memory中读取桶
					Long pos = bucketDirectMemList.get(bucketKey);
					if( pos != null) {
						// 说明桶在directMemory里了
						byte[] bucketbytes;
						bucketbytes = directMemory.get(pos.intValue(), memoryType);
						
						fileBucket = (HashBucket<K,T>)kryoContext.deserialze(
								HashBucket.class, bucketbytes);
						fileBucket.setContext(this);
						//input.close();
						// 不用放到桶队列里去 用完就删
					}
					else {
						//从文件里读
						// 下面从文件里读取桶
						if(bucketAddressList == null) {
							bucketAddressList = getHashDiskTable(bucketAddressOffset);
						}
						RandomAccessFile reader = bucketReaderPool.take();
						reader.seek(bucketAddressList.get(bucketKey));
						byte[] bucketByteArray = null;
						if( bucketKey == bucketNum -1 ) {
							bucketByteArray = new byte[lastObjectSize];
						}
						else {
							bucketByteArray = new byte[(int) (bucketAddressList.get(bucketKey + 1) - 
		                           bucketAddressList.get(bucketKey))];
						}
						
						reader.read(bucketByteArray);
						
						fileBucket = (HashBucket<K,T>)kryoContext.deserialze(
								HashBucket.class, bucketByteArray);
						
						//input.close();
						fileBucket.setContext(this);
						bucketReaderPool.add(reader);
						// 从文件里读的桶 才注册到桶管理器里
						bucketCachePool.addBucket(fileBucket);
						//bucketList.put(bucketKey, fileBucket);
					}
				}
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
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return fileBucket;

	}
	
	/**
	 * 将小表商品id和买家id的索引的key值对应的byte列表加上offset值
	 * @param key
	 * @param offset
	 */
	public void putOffset( K key, byte[] appendOffset) {
		// 先拿到相应的桶
		HashBucket<K,T> bucket = null;
		int bucketIndex = getBucketIndex( key);
		if (bucketIndex < bucketNum) {
			bucket = readBucket((int) bucketIndex);
		} else {
			bucket = readBucket((int) (bucketIndex % Math.pow(10,
					usedBits - 1)));
		}
		// 拿完桶了
		if (bucket != null) {
			List<byte[]> offsets = bucket.getAddress(getBucketStringIndex( key), key);
			for( byte[] offset : offsets) {
				// 根据offset得到在直接内存里的地址  如果没有 则新创建一个直接内存地址  并加入appendOffset进去
				ByteBuffer byteBuffer = ByteBuffer.wrap(offset);
				if( offset.length > RaceConfig.compressed_min_bytes_length) {
					// 说明已经有直接内存地址了
					byteBuffer.position(RaceConfig.compressed_min_bytes_length);
					int pos = byteBuffer.getInt();				// 得到直接内存地址
					byte[] originByte = null;
					try {
						originByte = LZFDecoder.decode(directMemory.get(pos, memoryType));
					} catch (LZFException e) {
						// TODO Auto-generated catch block
						originByte = offset;
					}
					ByteBuffer buffer = ByteBuffer.allocate(originByte.length + 
							RaceConfig.compressed_min_bytes_length);
					// 下面将appendOffset加入byte[]数组里 并压缩存储
					buffer.put(originByte);
					buffer.put(appendOffset);
					// 排序
					byte[] compressBytes = LZFEncoder.encode(buffer.array());
					if( compressBytes.length > RaceConfig.compressed_remaining_bytes_length) {
						// 说明超过了最大预留空间  需要写到尾部去
						int newPos = directMemory.putAndAppendRemaining(compressBytes, memoryType);
						if( newPos != -1) {
							// 重新写入成功
							byteBuffer.position(RaceConfig.compressed_min_bytes_length);
							byteBuffer.putInt(newPos);
							bucket.replaceAddress(getBucketStringIndex( key), key,(T)byteBuffer.array());
						}
					}
					else {
						// 重新写入直接内存中
						directMemory.putInSprcificPos(compressBytes,pos, memoryType);
					}
					//bucket.replaceAddress(getBucketStringIndex(key), key, compressBytes);
				}
				else {
					// 说明还没有创建直接内存地址  给它创建一个
					ByteBuffer newBuffer = ByteBuffer.allocate(appendOffset.length);
					// 第一个放进去的无所谓压不压缩了
					newBuffer.put(appendOffset);
					int pos = directMemory.putAndAppendRemaining(
							LZFEncoder.encode(newBuffer.array()), memoryType);
					if( pos != -1) {
						// 说明写成功了  将地址放到offset的后面
						byteBuffer = ByteBuffer.allocate(RaceConfig.compressed_min_bytes_length + 4).
							put(offset).putInt(pos);
						bucket.replaceAddress(getBucketStringIndex(key), key, (T)byteBuffer.array());
					}
				}
				
			}
			
		} else {
			// need to read from file
			System.out.println("read error!");
		}
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
			memRecordNum ++;
			if (++recordNum / bucketNum > RaceConfig.hash_index_block_capacity * 0.8) {
				// 增加新桶
				HashBucket<K,T> newBucket = new HashBucket<K,T>(this, bucketNum, classType);
				// 注册桶
				//bucketWriterWhenBuilding.addBucket(newBucket);
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

	/*public static void main(String args[]){
		ByteDirectMemory directMemory = new ByteDirectMemory(1024*1024);
		DiskHashTable<Integer, List<byte[]>> table = new DiskHashTable<Integer,List<byte[]>>
		("1.txt",List.class, DirectMemoryType.MainSegment);
		Random random = new Random();
		int key = 0;
		for( int j = 0; j < 5000; j++) {
			table.put(key, UUID.randomUUID().toString().getBytes());
		}
		
		long startTime = System.currentTimeMillis();

        //serialise object
		HashBucket bucket = table.bucketList.get(0);
        //try-with-resources used to autoclose resources
		ByteArrayOutputStream byteArrayOs = new ByteArrayOutputStream();
		
       Output output = new Output(byteArrayOs);
       Kryo kryo=new Kryo();
       kryo.writeClassAndObject(output, bucket);
       output.close();
        //deserialise object

        HashBucket<Integer, List<byte[]>> retrievedObject=null;
        Input input = new Input(byteArrayOs.toByteArray());
            Kryo kryo2=new Kryo();
            retrievedObject=(HashBucket<Integer, List<byte[]>>)kryo2.readClassAndObject(input);

        System.out.println("Retrieved from file: " + retrievedObject.toString());
		for( int i = 0; i< 2000; i++) {
			table.writeBucketWhenBuilding(key);
			List<byte[]> list = table.get(key);
			for (byte[] temp : list) {
				//System.out.println(new String(temp));
			}
		}
		
		long endTime = System.currentTimeMillis();
		System.out.println("time:" + (endTime - startTime));
		
 	}*/

}
