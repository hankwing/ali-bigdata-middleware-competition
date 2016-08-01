package com.alibaba.middleware.race;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.middleware.cache.ConcurrentCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.handlefile.ConstructSystem;
import com.alibaba.middleware.handlefile.DataFileMapping;
import com.alibaba.middleware.handlefile.FileIndexWithOffset;
import com.alibaba.middleware.index.ByteDirectMemory;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.threads.*;
import com.alibaba.middleware.tools.ByteUtils;
import com.alibaba.middleware.tools.BytesKey;
import com.alibaba.middleware.tools.RecordsUtils;

/**
 * 订单系统实现
 * 
 * @author hankwing
 *
 */
public class OrderSystemImpl implements OrderSystem {

	// 存订单表里的orderId索引<索引文件的下标,内存里缓存的索引DiskHashTable>
	public ConcurrentHashMap<Integer, DiskHashTable<Long>> orderIdIndexList = null;
	// 订单表里的buyerId代理键索引
	//public ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>> orderBuyerIdIndexList = null;
	// 订单表里的goodId代理键索引
	//public ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>> orderGoodIdIndexList = null;
	// 订单表里的可计算字段索引Map
	//public ConcurrentHashMap<Integer, List<DiskHashTable<Integer, List<byte[]>>>> orderCountableIndexList = null;
	// buyerId里的buyerId代理键索引
	public ConcurrentHashMap<Integer, DiskHashTable<BytesKey>> buyerIdIndexList = null;
	// goodId里的goodId代理键索引
	public ConcurrentHashMap<Integer, DiskHashTable<BytesKey>> goodIdIndexList = null;
	// 文件句柄池<数据文件的下标，文件句柄队列>
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> orderHandlersList = null;
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> buyerHandlersList = null;
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> goodHandlersList = null;
	// 存所有buyerid或者goodid对应的orderid list的文件句柄池
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> 
	buyerOrderIdListHandlersList = null;
	// 存所有buyerid或者goodid对应的orderid list的文件句柄池
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> 
	goodOrderIdListHandlersList = null;

	//public CopyOnWriteArrayList<FilePathWithIndex> orderFileList = null; 
	//public CopyOnWriteArrayList<FilePathWithIndex> buyerFileList = null; 
	//public CopyOnWriteArrayList<FilePathWithIndex> goodFileList = null; 
	
	public DataFileMapping orderFileMapping = null;						// 保存order表所有文件的名字
	public DataFileMapping buyerFileMapping = null;						// 保存buyer表所有文件的名字
	public DataFileMapping goodFileMapping = null;						// 保存good表所有文件的名字
	
	public DataFileMapping buyerOrderIdListMapping = null;						// 保存orderid列表的所有文件的名字
	public DataFileMapping goodOrderIdListMapping = null;						// 保存orderid列表的所有文件的名字
	public DataFileMapping orderIndexMapping = null;						// 保存order表所有文件的名字
	public DataFileMapping buyerIndexMapping = null;						// 保存buyer表所有文件的名字
	public DataFileMapping goodIndexMapping = null;						// 保存good表所有文件的名字

	//public HashSet<String> orderAttrList = null; // 保存order表的所有字段名称
	public HashSet<String> buyerAttrList = null; // 保存buyer表的所有字段名称
	public HashSet<String> goodAttrList = null; // 保存good表的所有字段名称

	//public FilePathWithIndex buyerIdSurrKeyFile = null; // 存代理键索引块的文件地址和索引元数据偏移地址
	//public FilePathWithIndex goodIdSurrKeyFile = null; // 存代理键索引块的文件地址和索引元数据偏移地址
	//public DiskHashTable<String, Long> buyerIdSurrKeyIndex = null; // 缓存buyerId事实键与代理键
	//public DiskHashTable<String, Long> goodIdSurrKeyIndex = null; // 缓存goodId事实键与代理键
	//public HashMap<Integer,Integer>  buyerIdSurrKeyIndex = null;
	//public HashMap<Integer,Integer>  goodIdSurrKeyIndex = null;

	private ThreadPool threadPool = ThreadPool.getInstance();
    private ExecutorService queryExe = threadPool.getQueryExe();
    public ConcurrentCache rowCache = null;
	private AtomicLong queryCounter = new AtomicLong(0L);
    private AtomicLong q1Sum = new AtomicLong(0L);
    private AtomicLong q2Sum = new AtomicLong(0L);
    private AtomicLong q3Sum = new AtomicLong(0L);
    private AtomicLong q4Sum = new AtomicLong(0L);
    private AtomicLong q1Counter = new AtomicLong(0L);
    private AtomicLong q2Counter = new AtomicLong(0L);
    private AtomicLong q3Counter = new AtomicLong(0L);
    private AtomicLong q4Counter = new AtomicLong(0L);

//    public static AtomicBoolean waitForConstruct = new AtomicBoolean(true);
    public AtomicLong waitForConstruct = new AtomicLong(0L);
	static List<String> buyerfiles = null;
	static List<String> goodfiles = null;
	static List<String> orderfiles = null;
	static OrderSystemImpl orderSystem = null;

	/**
	 * 测试类 construct测试construct方法
	 * @param args
	 */
	public static void main(String[] args) {
		orderSystem = new OrderSystemImpl();
		Scanner scanner = new Scanner(System.in);
		String command = null;
		
		
		while (!(command = scanner.nextLine()).equals("quit")) {
			try{
				if (command.equals("write")) {
					// write 将内存中的索引文件写出去
	
				} else if (command.startsWith("writeBucket")) {
					// writeBucket:xx 将某个索引块xx写到外存 索引块号xx可在索引块内找到
					
				} else if (command.equals("read")) {
					// read 从文件中读取索引元数据DiskHashTable
	
				} else if (command.equals("construct")) {
					// 在内存中建立orderBench.txt的索引 建立期间可随时调用write将某个块写出去
	
					buyerfiles = new ArrayList<String>();
					buyerfiles.add("prerun_data/buyer.0.0");
					buyerfiles.add("prerun_data/buyer.1.1");
//					buyerfiles.add("benchmark/buyer_records_1.txt");
					//buyerfiles.add("benchmark/buyer_records_2.txt");
	
					goodfiles = new ArrayList<String>();
					goodfiles.add("prerun_data/good.0.0");
					goodfiles.add("prerun_data/good.1.1");
					goodfiles.add("prerun_data/good.2.2");
//					goodfiles.add("benchmark/good_records_1.txt");
//					goodfiles.add("benchmark/good_records_2.txt");
//					goodfiles.add("benchmark/good_records_3.txt");
//					goodfiles.add("benchmark/good_records_4.txt");
//					goodfiles.add("benchmark/good_records.txt");
//					goodfiles.add("benchmark/good_records_1.txt");
	
					orderfiles = new ArrayList<String>();

					orderfiles.add("disk1/orders/order.0.0");
					orderfiles.add("disk2/orders/order.0.3");
					orderfiles.add("disk3/orders/order.1.1");
					orderfiles.add("disk1/orders/order.2.2");

//					for( int i = 0; i <30; i++) {
//						orderfiles.add("benchmark/order_records_"+ i + ".txt");
//					}
					
//					orderfiles.add("benchmark/order_records_2.txt");
//					orderfiles.add("benchmark/order_records_4.txt");
//					orderfiles.add("benchmark/order_records_5.txt");
//					orderfiles.add("benchmark/order_records_6.txt");
//					orderfiles.add("benchmark/order_records_9.txt");
//					orderfiles.add("benchmark/order_records_12.txt");
//					orderfiles.add("benchmark/order_records_18.txt");
//					orderfiles.add("benchmark/order_records_19.txt");
//					orderfiles.add("benchmark/order_records_20.txt");
//					orderfiles.add("benchmark/order_records_23.txt");
//					orderfiles.add("benchmark/order_records_24.txt");
//					orderfiles.add("benchmark/order_records_26.txt");
//					orderfiles.add("benchmark/order_records_27.txt");
//					orderfiles.add("benchmark/order_records_28.txt");
//					orderfiles.add("benchmark/order_records_29.txt");
//	
					List<String> storeFolders = new ArrayList<String>();
					// 添加三个盘符
					storeFolders.add("disk1/");
					storeFolders.add("disk2/");
					storeFolders.add("disk3/");
	
					try {
						orderSystem.construct(orderfiles, buyerfiles, goodfiles,
								storeFolders);
	
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				} else if (command.startsWith("lookup1")) {
	
					// lookup:xxx 查找某个key值的value
					String[] rawCommand = command.substring(command.indexOf(":") + 1).split(",");
					List<String> keys = new ArrayList<String>();
					for( int i = 1; i < rawCommand.length; i++ ) {
	                    System.out.println(rawCommand[i]);
						keys.add(rawCommand[i]);
					}
					System.out.println("values:" + 
					orderSystem.queryOrder( Long.valueOf(rawCommand[0]), keys));
//					for( int i = 0; i < 8; i++) {
//						// 启动八个线程同时查询
//						Thread query = new Thread(new Runnable() {  
//						    @Override  
//						    public void run() {  
//						    	List<String> keys = new ArrayList<String>();
//								keys.add("orderid");
//								int count = 0;
//								for( int i = 0; i < 1 ; i++) {
//									FileInputStream fis;
//									try {
//										fis = new FileInputStream(orderfiles.get(i));
//										BufferedReader br = new BufferedReader(new InputStreamReader(fis));
//									    String line = br.readLine();
//									    
//									    while( line != null && count < 3000) {
//									    	long orderid = Long.parseLong(RecordsUtils.getValueFromLine(
//									    			line, RaceConfig.orderId));
//									    	if(orderSystem.queryOrder( orderid, keys) == null) {
//									    		// error
//									    		count ++;
//									    		System.out.println("cannot find orderid:" + orderid);
//									    	}
//									    	line = br.readLine();
//									    }
//									    br.close();
//									} catch (IOException e) {
//										// TODO Auto-generated catch block
//										e.printStackTrace();
//									}
//								    
//								   
//								}
//								System.out.println("error count:" + count);                
//						    };  
//						});  
//						
//						query.start();
//					}
					
				}  else if (command.startsWith("lookup2")) {
					// lookup:xxx 查找某个key值的value
					String[] rawCommand = command.substring(command.indexOf(":") + 1).split(",");
					String buyerId = rawCommand[0];
					long startTime = Long.valueOf(rawCommand[1]);
					long endTime = Long.valueOf(rawCommand[2]);
					
					Iterator<Result> results = orderSystem.queryOrdersByBuyer(startTime, endTime, buyerId);
					while(results.hasNext()) {
						System.out.println("values:" + results.next());
					}
					System.out.println("start query2" );
//					for( int i = 0; i < 8; i++) {
//						// 启动八个线程同时查询
//						Thread query = new Thread(new Runnable() {  
//						    @Override  
//						    public void run() {  
//						    	Random random = new Random();
//								FileInputStream fis;
//								try {
//									fis = new FileInputStream(buyerfiles.get(
//											random.nextInt(buyerfiles.size())));
//									 BufferedReader br = new BufferedReader(new InputStreamReader(fis));
//										for( int i = 0; i< 2000; i++) {
//											String buyerId = RecordsUtils.getValueFromLine(br.readLine(), RaceConfig.buyerId);
//											buyerId = buyerId == null? UUID.randomUUID().toString():buyerId;
//											long startTime = random.nextLong();
//											long endTime = random.nextLong();
//											
//											Iterator<Result> results = orderSystem.queryOrdersByBuyer(startTime, endTime, buyerId);
//											System.out.println("query2");
//											while(results.hasNext()) {
//												System.out.println("values:" + results.next());
//											}
//										}
//										System.out.println("end query2");
//										br.close();
//								} catch (IOException e) {
//									// TODO Auto-generated catch block
//									e.printStackTrace();
//								}
//							                  
//						    };  
//						});  
//						
//						query.start();
//					}
					
				} else if (command.startsWith("lookup3")) {
					// lookup:xxx 查找某个key值的value
					
//					for( int i = 0; i < 8; i++) {
//						// 启动八个线程同时查询
//						Thread query = new Thread(new Runnable() {  
//						    @Override  
//						    public void run() {  
//						    	Random random = new Random();
//								try {
//									System.out.println("start query3" );
//									List<String> keys = new ArrayList<String>();
//									keys.add("orderid");
//									int count = 0;
//									for( int i = 0; i < orderfiles.size() && count < 2000 ; i++) {
//										FileInputStream fis = new FileInputStream(orderfiles.get(i));
//									    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
//									    String line = br.readLine();
//									    
//									    while( line != null) {
//									    	String goodid = RecordsUtils.getValueFromLine(
//									    			line, RaceConfig.goodId);
//									    	Iterator<Result> results = orderSystem.queryOrdersBySaler("", goodid, keys);
//											//System.out.println("query3");
//											if(results.hasNext()) {
//												System.out.println(ConcurrentCache.getInstance().getSize());
//											}
//											else {
//												count ++;
//												
//												System.out.println("error");
//											}
//									    	line = br.readLine();
//									    }
//									    br.close();
//									   
//									}
//									
//									System.out.println("error count:" + count);
//								} catch (IOException e) {
//									// TODO Auto-generated catch block
//									e.printStackTrace();
//								}
//							                  
//						    };  
//						});  
//						
//						query.start();
//					}
					/*Random random = new Random();
					FileInputStream fis = new FileInputStream(orderfiles.get(random.nextInt(
							orderfiles.size())));
				    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
					for( int i = 0; i< 2000; i++) {
						String goodId = RecordsUtils.getValueFromLine(br.readLine(), RaceConfig.goodId);
						goodId = goodId == null? UUID.randomUUID().toString(): goodId;
						Iterator<Result> results = orderSystem.queryOrdersBySaler("", goodId, null);
						System.out.println("query3");
						if(results.hasNext()) {
							
						}
						else {
							System.out.println("error");
						}
						
					}
					System.out.println("stop query3" );*/
					//br.close();
					String[] rawCommand = command.substring(command.indexOf(":") + 1).split(",");
					String goodId = rawCommand[0];
					List<String> keys = new ArrayList<String>();
					for( int i = 1; i < rawCommand.length; i++ ) {
						keys.add(rawCommand[i]);
					}
					int count = 0;
					Iterator<Result> results = orderSystem.queryOrdersBySaler("", goodId, keys);
					while(results.hasNext()) {
						count ++;
						//results.next();
						System.out.println("values:" + results.next());
					}
					System.out.println("count:" + count);
//					
				} else if (command.startsWith("lookup4")) {
					// lookup:xxx 查找某个key值的value
					// lookup:xxx 查找某个key值的value
					
					String[] rawCommand = command.substring(command.indexOf(":") + 1).split(",");
					String goodId = rawCommand[0];
					String key = rawCommand[1];
					System.out.println(orderSystem.sumOrdersByGood(goodId, key));
//					Random random = new Random();
//					System.out.println("start query4" );
//					FileInputStream fis = new FileInputStream(goodfiles.get(
//							random.nextInt(goodfiles.size())));
//				    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
//					String[] keys = orderSystem.buyerAttrList.toArray(new String[0]);
//					for( int i = 0; i< 2000; i++) {
//						
//						String goodId = RecordsUtils.getValueFromLine(br.readLine(), RaceConfig.goodId);
//						goodId = goodId == null? UUID.randomUUID().toString(): goodId;
//						//System.out.println("query4");
//						System.out.println(orderSystem.sumOrdersByGood(goodId, keys[random.nextInt(keys.length -1)]));
//					}		
//					br.close();
//					System.out.println("end query4" );
					
				} else if (command.equals("quit")) {
					// 索引使用完毕 退出
					
				}
			} catch( Exception e) {
				e.printStackTrace();
				continue;
			}
		}
		
		

		scanner.close();

	}

	public OrderSystemImpl() {
		// 初始化操作

		// 存订单表里的orderId索引<文件名（尽量短名）,内存里缓存的索引DiskHashTable>
		orderIdIndexList = new ConcurrentHashMap<Integer, DiskHashTable<Long>>();
		// 订单表里的buyerId代理键索引
		//orderBuyerIdIndexList = new ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>>();
		// 订单表里的goodId代理键索引
		//orderGoodIdIndexList = new ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>>();
		// 订单表里的可计算字段索引Map
		//orderCountableIndexList = new ConcurrentHashMap<Integer, List<DiskHashTable<Integer, List<byte[]>>>>();
		// buyerId里的buyerId代理键索引
		buyerIdIndexList = new ConcurrentHashMap<Integer, DiskHashTable<BytesKey>>();
		// goodId里的goodId代理键索引
		goodIdIndexList = new ConcurrentHashMap<Integer, DiskHashTable<BytesKey>>();
		orderHandlersList = new ConcurrentHashMap<Integer,LinkedBlockingQueue<RandomAccessFile>>();
		buyerHandlersList = new ConcurrentHashMap<Integer,LinkedBlockingQueue<RandomAccessFile>>();
		goodHandlersList = new ConcurrentHashMap<Integer,LinkedBlockingQueue<RandomAccessFile>>();
		buyerOrderIdListHandlersList = new 
				ConcurrentHashMap<Integer,LinkedBlockingQueue<RandomAccessFile>>();
		goodOrderIdListHandlersList = new 
				ConcurrentHashMap<Integer,LinkedBlockingQueue<RandomAccessFile>>();
		
		orderFileMapping = new DataFileMapping();
		buyerFileMapping = new DataFileMapping();
		goodFileMapping = new DataFileMapping();
		
		buyerOrderIdListMapping = new DataFileMapping();
		goodOrderIdListMapping = new DataFileMapping();
		orderIndexMapping = new DataFileMapping();
		buyerIndexMapping = new DataFileMapping();
		goodIndexMapping = new DataFileMapping();

		//orderAttrList = new HashSet<String>(); // 保存order表的所有字段名称
		buyerAttrList = new HashSet<String>(); // 保存buyer表的所有字段名称
		goodAttrList = new HashSet<String>(); // 保存good表的所有字段名称
		//buyerIdSurrKeyFile = new FilePathWithIndex(); // 存代理键索引块的文件地址和索引元数据偏移地址
		//goodIdSurrKeyFile = new FilePathWithIndex();

		//JVMMonitorThread jvmMonitorThread = new JVMMonitorThread("JVMMonitor");
		//CacheMonitorThread cacheMonitorThread = new CacheMonitorThread(ConcurrentCache.getInstance());
		//threadPool.addWorker(FIFOCacheMonitorThread.getInstance());
		//threadPool.startWorkers();
		
	}

	/**
	 * 
	 * 读每种类型的文件 直接写到小文件里 处理逻辑由单独线程处理
	 *
	 */
	public void construct(Collection<String> orderFiles,
			Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException,
			InterruptedException {
		// 将存储目录存起来 之后建小文件及索引文件的时候用
		RaceConfig.storeFolders = storeFolders.toArray(new String[0]);
		long startTime = System.currentTimeMillis();

		ConstructSystem constructSystem = new ConstructSystem(this);
		constructSystem.startHandling(buyerFiles, goodFiles, orderFiles,
				storeFolders, RaceConfig.handleThreadNumber);

		long endTime = System.currentTimeMillis();
		rowCache = ConcurrentCache.getInstance();	// 这时候再开启rowCache
		System.out.println("construct time:" + (endTime - startTime) / 1000);
	}

	/**
	 * 根据文件路径和偏移读取索引元数据
	 * 
	 * @param filePath
	 * @param offSet
	 * @return
	 */
	public DiskHashTable getHashDiskTable(String filePath, long offSet) {
		DiskHashTable hashTable = null;
		FileInputStream streamIn;
		try {
			streamIn = new FileInputStream(filePath);
			streamIn.getChannel().position(offSet);
			ObjectInputStream objectinputstream = new ObjectInputStream(
					streamIn);

			hashTable = (DiskHashTable) objectinputstream.readObject();
			hashTable.restore();
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
		return hashTable;

	}

	/**
	 * 将buyerId和goodId的事实键转化为代理键
	 * 
	 * @param id
	 * @return
	 */
	/*@SuppressWarnings("unchecked")
	public long getSurrogateKey(String id, IdName idName) {
		long surrogateKey = 0;
		switch (idName) {
		case OrderId:
			break;
		case BuyerId:
			//if (buyerIdSurrKeyIndex == null) {
			//	buyerIdSurrKeyIndex = getHashDiskTable(
			//			buyerIdSurrKeyFile.getFilePath(),
			//			buyerIdSurrKeyFile.getSurrogateIndex());
			//}
			List<Long> buyerIdresult = buyerIdSurrKeyIndex.get(id);
			if( buyerIdresult.size() != 0) {
				surrogateKey = buyerIdresult.get(0);
			}
			
			break;
		case GoodId:
			if (goodIdSurrKeyIndex == null) {
				goodIdSurrKeyIndex = getHashDiskTable(
						goodIdSurrKeyFile.getFilePath(),
						goodIdSurrKeyFile.getSurrogateIndex());
			}
			List<Long> goodIdresult = goodIdSurrKeyIndex.get(id);
			if( goodIdresult.size() != 0) {
				surrogateKey = goodIdresult.get(0);
			}
			break;
		}

		return surrogateKey;
	}*/
	
	/**
	 * 判断是否存在该orderid对应的记录
	 * 
	 * @return
	 * @throws TypeException 
	 */
	public boolean isRecordExist(long orderid) throws TypeException {
		
		for (int filePathIndex : orderIndexMapping.getAllFileIndexs()) {
			DiskHashTable<Long> hashTable = orderIdIndexList.get(filePathIndex);
			byte[] results = hashTable.get(orderid);
			
			if (results != null) {
				// find the records offset

				// 解码获得long型的offset
				ByteBuffer buffer = ByteBuffer.wrap(results);
				int dataFileIndex = ByteUtils.getMagicIntFromByte(buffer.get());
				long offset = ByteUtils.getLongOffset(buffer.getInt());
					// 从文件里读数据
					String diskValue = RecordsUtils.getStringFromFile(
							orderHandlersList.get(dataFileIndex), offset, TableName.OrderTable);
					if( Long.parseLong(
							RecordsUtils.getValueFromLine(diskValue, RaceConfig.orderId))  == orderid) {
						return true;
					}

				break;
			}
		}
		
		return false;
	}


	/**
	 * 根据各个表的主键查找索引返回相应的表记录 无记录则返回null
	 * 
	 * @return
	 * @throws TypeException 
	 */
	public String getRowStringById(TableName tableName, Object id) throws TypeException {
		String idString = String.valueOf(id);
		switch (tableName) {
		case OrderTable:
			for (int fileIndex : orderIndexMapping.getAllFileIndexs()) {
				long orderid = Long.valueOf(idString);
				DiskHashTable<Long> hashTable = orderIdIndexList.get(fileIndex);
				byte[] results = hashTable.get(orderid);
				if (results != null) {
					// find the records offset
						//解码byte数组
					ByteBuffer buffer = ByteBuffer.wrap(results);
					int dataFileIndex = ByteUtils.getMagicIntFromByte(buffer.get());
					long offset = ByteUtils.getLongOffset(buffer.getInt());
					String diskValue = RecordsUtils.getStringFromFile(
							orderHandlersList.get(dataFileIndex),offset, tableName);
					if( RecordsUtils.getValueFromLine(diskValue, RaceConfig.orderId).equals(idString)) {
						// 确认主键相同
						return diskValue;
					}

				}
			}	
			break;
		case BuyerTable:
			// 先在缓冲区里找
			BytesKey key = new BytesKey(String.valueOf(id).getBytes());
			String result = rowCache.getFromCache(key, TableName.BuyerTable);
			if( result != null) {
				// 在缓冲区找到了
				return result;
			}
			else {
				// 在索引里找
				for (int filePathIndex : buyerIndexMapping.getAllFileIndexs()) {
					DiskHashTable<BytesKey> hashTable = buyerIdIndexList
							.get(filePathIndex);
					byte[] results = hashTable.get(key);
					if (results != null) {
						// 这里要解压出第一个offset出来
						ByteBuffer buffer = ByteBuffer.wrap(results);
						// 跳过标志位
						buffer.position(RaceConfig.byte_size);
						// 从byte解析出int
						int dataFileIndex = ByteUtils.getMagicIntFromByte(buffer.get());
						long offset = ByteUtils.getLongOffset(buffer.getInt());
						String records = RecordsUtils.getStringFromFile(
								buyerHandlersList.get(dataFileIndex), offset, tableName);
						try{
							// 放入缓冲区
							rowCache.putInCache(key, records, TableName.BuyerTable);
							return records;
						} catch(StringIndexOutOfBoundsException e){
							continue;
						}
					}

				}
			}
			break;
		case GoodTable:
			BytesKey goodKey = new BytesKey(String.valueOf(id).getBytes());
			// 先在缓冲区里找
			String goodResult = rowCache.getFromCache(goodKey, tableName);
			if( goodResult != null) {
				return goodResult;
			}
			else {
				for (int filePathIndex : goodIndexMapping.getAllFileIndexs()) {
					DiskHashTable<BytesKey> hashTable = goodIdIndexList.get(filePathIndex);
					byte[] results = hashTable.get(goodKey);
				if (results != null) {

						ByteBuffer buffer = ByteBuffer.wrap(results);
						// 跳过标志位
						buffer.position(RaceConfig.byte_size);
						// 从byte解析出int
						int dataFileIndex = ByteUtils.getMagicIntFromByte(buffer.get());
						long offset = ByteUtils.getLongOffset(buffer.getInt());
						String records = RecordsUtils.getStringFromFile(
								goodHandlersList.get(dataFileIndex), offset, tableName);
						try{
							// 确认主键相同
							rowCache.putInCache(goodKey, records, TableName.GoodTable);
							return records;
						} catch(StringIndexOutOfBoundsException e){
							continue;
						}
				}

				}
			}
			break;
		}
		if( tableName == TableName.BuyerTable || tableName == TableName.GoodTable) {
			System.out.println("read error: have no specific data with id:" + idString);
		}
		
		return null; 
	}

	/**
	 * 查询订单号为orderid的指定字段
	 * 
	 * @param orderId
	 *            订单号
	 * @param keys
	 *            待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
	 * @return 查询结果，如果该订单不存在，返回null
	 */
	public ResultImpl queryOrder(long orderId, Collection<String> keys) {
		// 主要思想：先判断keys在哪个表里 之后根据索引在不同表里找不同字段
		ResultImpl result = null;
        if (queryExe != null) {
            long before = System.currentTimeMillis();
            QueryOrderThread t = new QueryOrderThread(this,orderId, keys);
            // 没构建完则等待一段时间
            while (waitForConstruct.get() < RaceConfig.orderTableThreadNum) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // DO NOTHING. WAIT FOR CONSTRUCTION
//				System.out.println("Q4 Waiting for construction");
            }
            Future<ResultImpl> future = queryExe.submit(t);
            try {
                result = future.get();
				queryCounter.getAndIncrement();
                q1Counter.getAndIncrement();
                long costTime = System.currentTimeMillis() - before;
                q1Sum.addAndGet(costTime);
//                System.out.println("Done query1: " + queryCounter.get() + ", Cost: " + costTime + "ms");
                System.out.println("Until now, done query1 " + q1Counter.get() + " average cost time: " + q1Sum.get() / q1Counter.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
		return result;
	}

	/**
	 * 查询某位买家createtime字段从[startTime, endTime) 时间范围内发生的所有订单的所有信息
	 * 
	 * @param startTime
	 *            订单创建时间的下界
	 * @param endTime
	 *            订单创建时间的上界
	 * @param buyerid
	 *            买家Id
	 * @return 符合条件的订单集合，按照createtime大到小排列
	 */
	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
			String buyerid) {

        Iterator<Result> iterator = null;
        if (queryExe != null) {
            long before = System.currentTimeMillis();
            QueryOrderByBuyerThread t = new QueryOrderByBuyerThread(this, startTime, endTime, buyerid);
            
            while (waitForConstruct.get() < RaceConfig.orderTableThreadNum) {
               
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // DO NOTHING. WAIT FOR CONSTRUCTION
//				System.out.println("Q4 Waiting for construction");
            }
            
            Future<Iterator<Result>> future = queryExe.submit(t);
            try {
                iterator = future.get();
				queryCounter.getAndIncrement();
                q2Counter.getAndIncrement();
                long costTime = System.currentTimeMillis() - before;
                q2Sum.getAndAdd(costTime);
                System.out.println("Done query2: " + queryCounter.get() + ", Cost: " + costTime + "ms");
                System.out.println("Until now, done query2 " + q2Counter.get() + " average cost time: " + q2Sum.get() / q2Counter.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        return iterator;
	}

	/**
	 * 查询某位卖家某件商品所有订单的某些字段
	 * 
	 * @param salerid
	 *            卖家Id
	 * @param goodid
	 *            商品Id
	 * @param keys
	 *            待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
	 * @return 符合条件的订单集合，按照订单id从小至大排序
	 */
	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
			Collection<String> keys) {

        Iterator<Result> iterator = null;
        if (queryExe != null) {
            long before = System.currentTimeMillis();
            QueryOrdersBySalerThread t = new QueryOrdersBySalerThread(this,salerid, goodid, keys);
            
            while (waitForConstruct.get() < RaceConfig.orderTableThreadNum) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // DO NOTHING. WAIT FOR CONSTRUCTION
//				System.out.println("Q4 Waiting for construction");
            }
            Future<Iterator<Result>> future = queryExe.submit(t);
            try {
                iterator = future.get();
				queryCounter.getAndIncrement();
                q3Counter.getAndIncrement();
                long costTime = System.currentTimeMillis() - before;
                q3Sum.getAndAdd(costTime);
                System.out.println("Done query3: " + queryCounter.get() + ", Cost: " + costTime + "ms");
                System.out.println("Until now, done query3 " + q3Counter.get() + " average cost time: " + q3Sum.get() / q3Counter.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        return iterator;
	}

	/**
	 * 对某件商品的某个字段求和，只允许对long和double类型的KV求和 如果字段中既有long又有double，则使用double
	 * 如果求和的key中包含非long/double类型字段，则返回null 如果查询订单中的所有商品均不包含该字段，则返回null
	 * 
	 * @param goodid
	 *            商品Id
	 * @param key
	 *            求和字段
	 * @return 求和结果
	 */
	@SuppressWarnings("unchecked")
	public KeyValue sumOrdersByGood(String goodid, String key) {
		// 根据商品ID找到多条订单信息 再根据key值加和
		KeyValueImpl result = null;

        if (queryExe != null) {
            long before = System.currentTimeMillis();
            SumOrdersByGoodThread t = new SumOrdersByGoodThread(this,goodid, key);
            
			while (waitForConstruct.get() < RaceConfig.orderTableThreadNum) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
				// DO NOTHING. WAIT FOR CONSTRUCTION
//				System.out.println("Q4 Waiting for construction");
			}
            Future<KeyValueImpl> future = queryExe.submit(t);
            try {
                result = future.get();
				queryCounter.getAndIncrement();
                q4Counter.getAndIncrement();
                long costTime = System.currentTimeMillis() - before;
                q4Sum.getAndAdd(costTime);
                System.out.println("Done query4: " + queryCounter.get() + ", Cost: " + costTime + "ms");
                System.out.println("Until now, done query4 " + q4Sum.get() + " average cost time: " + q4Sum.get() / q4Counter.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
		return result;
	}

}
