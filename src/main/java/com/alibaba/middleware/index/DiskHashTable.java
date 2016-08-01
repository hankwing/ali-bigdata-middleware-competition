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
import java.util.Arrays;
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
import com.alibaba.middleware.handlefile.DataFileMapping;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.tools.BufferedRandomAccessFile;
import com.alibaba.middleware.tools.ByteUtils;
import com.alibaba.middleware.tools.RecordsUtils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

/**
 * 索引元信息 保存桶数、记录数、使用的位数、桶对应的物理地址等信息 缓冲区管理调用的writeBucket是线程安全的
 * 支持：所有String类型的key，Long型的value, 一key可对应多value
 * 
 * @author hankwing
 *
 */
public class DiskHashTable<K> implements Serializable {

	private static final long serialVersionUID = 6020895636934444399L;
	private int usedBits;
	private int bucketNum;
	public long recordNum;
	public long memRecordNum;					// 内存里保留的记录条数  根据这个指标判断是否将一些桶写入direct memory
	// 保存数据的外村文件路径
	private String bucketFilePath = null; 
	private ReentrantReadWriteLock readWriteLock = null;

	// 保存桶数据 但一加载此类时这个Map是空的 当调用查询时才会从物理地址里load进相应的桶数据
	public transient Map<Integer, HashBucket<K>> bucketList = null;
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
	
	public DataFileMapping buyerOrderIdListMapping = null;						// 保存orderid列表的所有文件的名字
	public DataFileMapping goodOrderIdListMapping = null;						// 保存orderid列表的所有文件的名字
	
	public int orderListFileSeriNum = 0;								// 根据这个创建不同的orderidlist文件
	public String orderListFilePrex = null;
	// 存所有buyerid或者goodid对应的orderid list的文件句柄池
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<BufferedRandomAccessFile>> 
		buyerOrderIdListHandlersList = null;
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<BufferedRandomAccessFile>> 
		goodOrderIdListHandlersList = null;
//	public static int appendCount = 0;
//	public static int buyerCount = 0;
//	public static int sumContentNum = 0;
	
	// 用于解析Byte数组的ByteBuffer
//	public ByteBuffer offSetByteBuffer = null;
//	public ByteBuffer offSetByteBuffer = null;
//	public ByteBuffer offSetByteBuffer = null;
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
	public DiskHashTable( OrderSystemImpl system, String bucketFilePath, DirectMemoryType memoryType){
		this.memoryType = memoryType;
		usedBits = 1;
		bucketNum = 10;
		recordNum = 0;
		memRecordNum = 0;
		this.bucketFilePath = bucketFilePath;
		bucketList = new ConcurrentHashMap<Integer, HashBucket<K>>();
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
		this.buyerOrderIdListMapping = system.buyerOrderIdListMapping;
		this.goodOrderIdListMapping = system.goodOrderIdListMapping;
		this.buyerOrderIdListHandlersList = system.buyerOrderIdListHandlersList;
		this.goodOrderIdListHandlersList = system.goodOrderIdListHandlersList;
		// 保存orderid列表的文件名前缀
		if( memoryType == DirectMemoryType.BuyerIdSegment ) {
			orderListFilePrex = RaceConfig.storeFolders[1] + RaceConfig.buyerOrderListFileNamePrex;
		}
		else if( memoryType == DirectMemoryType.GoodIdSegment ) {
			orderListFilePrex = RaceConfig.storeFolders[2] + RaceConfig.goodOrderListFileNamePrex;
		}
		
		kryoContext = KryoContext.newKryoContextFactory(new KryoClassRegistrator(){
		    @Override
		    public void register(Kryo kryo) {
		        kryo.register(HashBucket.class);  
		    }       
		});
		
		for (int i = 0; i < 10; i++) {
			HashBucket<K> newBucket = new HashBucket<K>(this, i);
			//bucketCachePool.addBucket(newBucket);
			//bucketWriterWhenBuilding.addBucket(newBucket);
			bucketList.put(i, newBucket );
		}
	}

	/**
	 * 从文件里读取此类时 调用restore恢复初始化一些数据
	 */
	public void restore() {
		bucketList = new ConcurrentHashMap<Integer, HashBucket<K>>();
		//bucketCachePool = BucketCachePool.getInstance();
	}
	
	/**
	 * 构建完成后  将一部分桶写到直接内存  可加快查询
	 * 返回值：此DiskHashTable被写入dataFile的哪个位置，方便之后调用
	 */
//	public boolean writeAllBucketsToDirectMemory( DirectMemoryType writeType) {
//		memoryType = writeType;
//		for (int key = 0; key < bucketNum ; key ++) {
//			//bucketList.remove(bucketKey);
//			if( !directMemory.isFull(memoryType) ) {
//				// 有空间  试图往里写 但不一定写成功
//				try {
//					if(bucketAddressList == null) {
//						bucketAddressList = getHashDiskTable(bucketAddressOffset);
//					}
//
//					RandomAccessFile reader = bucketReaderPool.take();
//					reader.seek(bucketAddressList.get(key));
//					
//					byte[] bucketByteArray = null;
//					if( key == bucketNum -1 ) {
//						bucketByteArray = new byte[lastObjectSize];
//					}
//					else {
//						bucketByteArray = new byte[(int) (bucketAddressList.get(key + 1) - 
//	                           bucketAddressList.get(key))];
//					}
//					reader.read(bucketByteArray);
//					int newPos = directMemory.put(bucketByteArray, memoryType);
//					if( newPos != -1 ) {
//						//如果写入成功 则放入另一个地址队列 不能覆盖文件的物理地址队列
//						bucketDirectMemList.put(key, (long) newPos);
//					}
//					else {
//						return false;
//					}
//					bucketReaderPool.put(reader);
//					
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//			else {
//				return false;
//			}
//
//		}
//		
//		return true;
//	}

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
				if( memoryType != DirectMemoryType.NoWrite ) {
					// 如果是订单表的三个索引  那么先写到直接内存
					//System.out.println("write to direct memory success:" + key);
				}
				else {
					// 否则写到文件里
					//System.out.println("write to file:" + key);
					HashBucket<K> writeBucket = readBucket(key);
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
				}
				
			}

			//buffer output stream flush to file
			bufferedFout.flush();
			// write this HashTable to dataFile and return offset
			bucketList = new ConcurrentHashMap<Integer, HashBucket<K>>();		// 清空map
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
//	public boolean writeBucketAfterBuilding(int bucketKey) {
//		
//		//bucketList.remove(bucketKey);
//		boolean isSuccess = false;
//		HashBucket<K> bucketToRemove = bucketList.get(bucketKey);
//		if( !directMemory.isFull(memoryType) ) {
//			// 有空间  试图往里写 但不一定写成功
//
//			byte[] objectByte = kryoContext.serialze(bucketToRemove);
//			int newPos = directMemory.put(objectByte, memoryType);
//			if( newPos != -1 ) {
//				//如果写入成功 则放入另一个地址队列 不能覆盖文件的物理地址队列
//				isSuccess = true;
//				bucketList.remove(bucketKey);
//				bucketDirectMemList.put(bucketKey, (long) newPos);
//			}
//			else {
//				
//				System.out.println("direct memory is full");
//			}
//		}
//		
//		return isSuccess;
//		
//		
//	}
	
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
	public HashBucket<K> readBucket(int bucketKey) {
		HashBucket<K> fileBucket = bucketList.get( bucketKey);
		try {
			if( fileBucket == null) {
				//readWriteLock.readLock().lock();
				if (isbuilding) {
					//从直接内存拿数据
//					readWriteLock.readLock().lock();
//					//System.out.println("read bucket from direct mem:" + bucketKey);
//					long startp = bucketAddressList.get(bucketKey);
//					byte[] bucketbytes;
//
//					bucketbytes = directMemory.get((int)startp, memoryType);
//
//					ObjectInputStream bucketReader = new ObjectInputStream(
//							new ByteArrayInputStream(bucketbytes));
//
//					fileBucket = (HashBucket<K>) bucketReader.readObject();
//					fileBucket.setContext(this);
//					bucketReader.close();
//					readWriteLock.readLock().unlock();

				} else {
					// 查询阶段  先从directMemory拿桶数据  拿不到再从文件中读取数据
					// 下面从direct memory中读取桶
//					Long pos = bucketDirectMemList.get(bucketKey);
//					if( pos != null) {
//						// 说明桶在directMemory里了
//						byte[] bucketbytes;
//						bucketbytes = directMemory.get(pos.intValue(), memoryType);
//						
//						fileBucket = (HashBucket<K>)kryoContext.deserialze(
//								HashBucket.class, bucketbytes);
//						fileBucket.setContext(this);
//						//input.close();
//						// 不用放到桶队列里去 用完就删
//					}
//					else {
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
						
						fileBucket = (HashBucket<K>)kryoContext.deserialze(
								HashBucket.class, bucketByteArray);
						
						//input.close();
						fileBucket.setContext(this);
						bucketReaderPool.add(reader);
						// 从文件里读的桶 才注册到桶管理器里
						bucketCachePool.addBucket(fileBucket);
						//bucketList.put(bucketKey, fileBucket);
//					}
				}
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
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
	 * 每个offset的形式为：byte 代表当前是否有内容在直接内存 + byte 代表本身的文件索引号 + int 文件里的偏移地址 + byte + offset +...
	 * @param key
	 * @param offset
	 */
	public void putOffset( K key, byte[] appendOffset) {
		// 先拿到相应的桶
		HashBucket<K> bucket = null;
		int bucketIndex = getBucketIndex( key);
		if (bucketIndex < bucketNum) {
			bucket = readBucket((int) bucketIndex);
		} else {
			bucket = readBucket((int) (bucketIndex % Math.pow(10,
					usedBits - 1)));
		}
		// 拿完桶了
		if (bucket != null) {
			// 对应改buyerid或者goodid的记录地址的offset
			
			byte[] offset = bucket.getAddress(getBucketStringIndex( key), key);
			if( offset != null) {
				boolean isNeedDump = false;
				
				if( offset[0] >= RaceConfig.byte_has_direct_memory_pos) {
					// 得到
					// 说明后面已经带有地址信息了  拿到最后一个地址信息 代表直接内存的pos
					byte[] byteAndOffset = Arrays.copyOfRange(offset, offset.length
							- RaceConfig.compressed_min_bytes_length, offset.length);
					//ByteBuffer byteAndPosBuffer = ByteBuffer.wrap(byteAndOffset);
					//byteAndPosBuffer.position(RaceConfig.byte_size);
					int memoryIndex = ByteUtils.getMagicIntFromByte(byteAndOffset[0]);
					// 跳过第一个字节
					int pos = ByteUtils.byteArrayToLeInt(Arrays.copyOfRange(byteAndOffset, 
							1, byteAndOffset.length));
					
					int size = directMemory.getByteSize( memoryIndex, pos);
					int maxSize = memoryType == DirectMemoryType.BuyerIdSegment ?
							directMemory.orderBuyerPreserveSpace / offset[0] :
								directMemory.orderGoodPreserveSpace / offset[0];
					if( size + appendOffset.length > maxSize) {
//						sumContentNum = sumContentNum + appendOffset.length + 
//								RaceConfig.buyer_remaining_bytes_length + 4;
						// 说明要将appendOffset放到另外一块direct memory当中  并且要更新当前索引里的信息
//						appendCount ++; 
						if( offset[0] > 3) {
							// 说明该id对应的orderid列表超过平均数了  可能是热点
							offset[0] = 1;
						}
						else {
							offset[0]++;
						}
						// 第一位代表写入的缓冲区编号  第二位代表写入缓冲区的位置
						int[] newPos = directMemory.putAndAppendRemaining(appendOffset, memoryType, 
								memoryType == DirectMemoryType.BuyerIdSegment ?
										directMemory.orderBuyerPreserveSpace / offset[0] :
											directMemory.orderGoodPreserveSpace / offset[0] );
						if( newPos[1] != -1) {
							// 说明写成功了  将地址放到offset的后面
							//byte sign = ByteUtils.getMagicByteFromInt(orderListFileSeriNum); //  sign代表文件下标
							
							byte[] combined = new byte[offset.length + RaceConfig.byte_size +
							                           RaceConfig.int_size];
							System.arraycopy(offset ,0,combined, 0,offset.length);
							combined[offset.length] = ByteUtils.getMagicByteFromInt(newPos[0]);
							byte[] intValue = ByteUtils.leIntToByteArray(newPos[1]);
							System.arraycopy(intValue ,0,combined, offset.length + RaceConfig.byte_size ,
									intValue.length);
							bucket.replaceAddress(getBucketStringIndex(key), key, combined);
						}
						else {
							// 说明direct memory内存不够了  将direct memory dump到文件里去 但写完后还要调用一次putoffset
							isNeedDump = true;
						}
						
					}
					else {
						// 说明还能放得下 更新direct memory 在尾部追加  appendOffset 并且更新大小标识
//						byte[] originByte = directMemory.get(pos, memoryType);
//						// 将新地址放到后面去
//						byte[] combined = new byte[originByte.length + appendOffset.length];
//						System.arraycopy(originByte,0,combined,0,originByte.length);
//						System.arraycopy(appendOffset,0, combined,originByte.length,appendOffset.length);
						if( memoryIndex == 2) {
							int a = 0;
						}
						directMemory.appendByteToPosAndUpdate(memoryIndex, appendOffset, pos, size);
					}
					//bucket.replaceAddress(getBucketStringIndex(key), key, compressBytes);
				}
				else {
					// 说明还没有创建直接内存地址  给它创建一个 在直接内存里放入offset并且得到pos 写入本身的索引当中
					//ByteBuffer newBuffer = ByteBuffer.allocate(appendOffset.length);
					//
					//newBuffer.put(sign);
					//newBuffer.put(appendOffset);
//					buyerCount ++;
//					sumContentNum = sumContentNum + appendOffset.length + 
//							RaceConfig.buyer_remaining_bytes_length + 4;
					int[] posInfo = directMemory.putAndAppendRemaining(appendOffset, memoryType, 
							memoryType == DirectMemoryType.BuyerIdSegment ?
									directMemory.orderBuyerPreserveSpace:
										directMemory.orderGoodPreserveSpace);
					if( posInfo[1] != -1) {
						// 说明写成功了  将地址放到offset的后面
						
						offset[0] = 1;				// 这里代表的是这个offset在直接内存里存在而且记录了当前有几个offset
						//byte sign = ByteUtils.getMagicByteFromInt(orderListFileSeriNum); //  sign代表文件下标
						
						byte[] combined = new byte[offset.length + RaceConfig.byte_size + 
						                           RaceConfig.int_size];
						System.arraycopy(offset ,0,combined, 0,offset.length);
						combined[offset.length] = ByteUtils.getMagicByteFromInt(posInfo[0]);//将写入的缓冲区编号写入
						byte[] intValue = ByteUtils.leIntToByteArray(posInfo[1]);
						System.arraycopy(intValue ,0,combined, offset.length + RaceConfig.byte_size ,
								intValue.length);
						bucket.replaceAddress(getBucketStringIndex(key), key, combined);
					}
					else {
						// 说明direct memory内存不够了  将direct memory dump到文件里去 但写完后还要调用一次putoffset
						isNeedDump = true;
					}
				}
				if( isNeedDump ) {
					// direct memory不应该不够
					System.out.println("direct memory full" );
					//System.exit(0);
//					dumpDirectMemory();
					// 写完文件后  还要再调用一次putoffset  将本次没有添加进去的内容添加到新的directmemory中
					//putOffset(key, appendOffset);
				}
			}
			
		} else {
			// need to read from file
			System.out.println("read error!");
		}
	}
	
	/**
	 * 将direct memory里的内容全部dump到文件里去
	 */
	/*public void dumpDirectMemory() {
		// 需要将direct memory dump到文件
		String orderListFileName = orderListFilePrex + orderListFileSeriNum;
		// 添加入orderListMapping
		try{
			// 写入文件里去
			RecordsUtils.writeToFile(orderListFileName, directMemory, memoryType );
			// 加入文件句柄
			switch( memoryType) {
			 case BuyerIdSegment:
				 orderListFileSeriNum = buyerOrderIdListMapping.addDataFileName(orderListFileName);
				 // 建立文件句柄
				LinkedBlockingQueue<RandomAccessFile> handlersQueue = 
						buyerOrderIdListHandlersList.get(orderListFileSeriNum);
				if( handlersQueue == null) {
					handlersQueue = new LinkedBlockingQueue<RandomAccessFile>();
					buyerOrderIdListHandlersList.put(orderListFileSeriNum, handlersQueue);
				}
				for( int i = 0; i < RaceConfig.fileHandleNumber ; i++) {
					handlersQueue.add(new RandomAccessFile(orderListFileName, "r"));
				}
				orderListFileSeriNum ++;
				 break;
			 case GoodIdSegment:
				 orderListFileSeriNum = goodOrderIdListMapping.addDataFileName(orderListFileName);
				// 建立文件句柄
				LinkedBlockingQueue<RandomAccessFile> goodHandlersQueue = 
						goodOrderIdListHandlersList.get(orderListFileSeriNum);
				if( goodHandlersQueue == null) {
					goodHandlersQueue = new LinkedBlockingQueue<RandomAccessFile>();
					goodOrderIdListHandlersList.put(orderListFileSeriNum, goodHandlersQueue);
				}
				for( int i = 0; i < RaceConfig.fileHandleNumber ; i++) {
					goodHandlersQueue.add(new RandomAccessFile(orderListFileName, "r"));
				}
				orderListFileSeriNum ++;
				break;
			 default:
				 break;
			 }
			
			// 写完之后  要将所有值的标志位置0 代表其在直接内存里没有值了
			for( HashBucket<K> bucket : bucketList.values()) {
				bucket.resetAllValuesSigns();
			}
			
		} catch( Exception e) {
			e.printStackTrace();
		}

	}*/

	/**
	 * 通过key值得到对应数据的地址偏移量
	 * 
	 * @param key
	 * @return
	 */
	public byte[] get(K key) {

		HashBucket<K> bucket = null;
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

		HashBucket<K> bucket = null;
		int bucketIndex = getBucketIndex(key);
		if (bucketIndex < bucketNum) {
			bucket = readBucket((int) bucketIndex);
		} else {
			bucket = readBucket((int) (bucketIndex % Math.pow(10,
					usedBits - 1)));
		}

		if (bucket != null) {			
			if( memoryType != DirectMemoryType.NoWrite) {
				// 对于buyer和good表的索引  需要写入一个标志位
				byte sign = 0;
				ByteBuffer signedBuffer = ByteBuffer.allocate(
						RaceConfig.byte_size + value.length).put(sign).put(value);
				bucket.putAddress(getBucketStringIndex(key), key, signedBuffer.array());
			}
			else {
				// order表索引就直接写就可以了
				bucket.putAddress(getBucketStringIndex(key), key, value);
			}
			
			memRecordNum ++;
			if (++recordNum / bucketNum > RaceConfig.hash_index_block_capacity * 0.8) {
				// 增加新桶
				HashBucket<K> newBucket = new HashBucket<K>(this, bucketNum);
				// 注册桶
				//bucketWriterWhenBuilding.addBucket(newBucket);
				bucketNum++;
				bucketList.put(bucketNum - 1, newBucket);

				if (bucketNum > Math.pow(10, usedBits)) {
					usedBits++;
				}

				int newBucketIndex = bucketNum - 1;
				HashBucket<K> modifyBucket = readBucket(
						(int) (newBucketIndex % Math.pow(10, usedBits - 1)));

				Map<K, byte[]> temp = 
						modifyBucket.getAllValues(String.valueOf(newBucketIndex));
				for (Iterator<Map.Entry<K, byte[]>> it = temp
						.entrySet().iterator(); it.hasNext(); ) {
					Map.Entry<K, byte[]> entry = it.next();
					if (getBucketIndex(entry.getKey()) == newBucketIndex) {
						newBucket.putAddress(getBucketStringIndex(entry.getKey()),entry.getKey(),
								entry.getValue());
						modifyBucket.minusRecordNum(1);
						it.remove();
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
