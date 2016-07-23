package com.alibaba.middleware.race;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.cache.ConcurrentCache;
import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.handlefile.ConstructSystem;
import com.alibaba.middleware.handlefile.DataFileMapping;
import com.alibaba.middleware.handlefile.FileIndexWithOffset;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.threads.*;
import com.alibaba.middleware.tools.RecordsUtils;

/**
 * 订单系统实现
 * 
 * @author hankwing
 *
 */
public class OrderSystemImpl implements OrderSystem {

	// 存订单表里的orderId索引<文件名（尽量短名）,内存里缓存的索引DiskHashTable>
	public ConcurrentHashMap<Integer, DiskHashTable<Long, byte[]>> orderIdIndexList = null;
	// 订单表里的buyerId代理键索引
	public ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>> orderBuyerIdIndexList = null;
	// 订单表里的goodId代理键索引
	public ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>> orderGoodIdIndexList = null;
	// 订单表里的可计算字段索引Map
	//public ConcurrentHashMap<Integer, List<DiskHashTable<Integer, List<byte[]>>>> orderCountableIndexList = null;
	// buyerId里的buyerId代理键索引
	public ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>> buyerIdIndexList = null;
	// goodId里的goodId代理键索引
	public ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>> goodIdIndexList = null;
	// 文件句柄池
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> orderHandlersList = null;
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> buyerHandlersList = null;
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> goodHandlersList = null;

	//public CopyOnWriteArrayList<FilePathWithIndex> orderFileList = null; 
	//public CopyOnWriteArrayList<FilePathWithIndex> buyerFileList = null; 
	//public CopyOnWriteArrayList<FilePathWithIndex> goodFileList = null; 
	
	public DataFileMapping orderFileMapping = null;						// 保存order表所有文件的名字
	public DataFileMapping buyerFileMapping = null;						// 保存buyer表所有文件的名字
	public DataFileMapping goodFileMapping = null;						// 保存good表所有文件的名字

	public HashSet<String> orderAttrList = null; // 保存order表的所有字段名称
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
    public SimpleCache rowCache = null;

	/**
	 * 测试类 construct测试construct方法
	 * @param args
	 */
	public static void main(String[] args) {
		OrderSystemImpl orderSystem = new OrderSystemImpl();
		Scanner scanner = new Scanner(System.in);
		String command = null;
		while (!(command = scanner.nextLine()).equals("quit")) {

			if (command.equals("write")) {
				// write 将内存中的索引文件写出去

			} else if (command.startsWith("writeBucket")) {
				// writeBucket:xx 将某个索引块xx写到外存 索引块号xx可在索引块内找到
				
			} else if (command.equals("read")) {
				// read 从文件中读取索引元数据DiskHashTable

			} else if (command.equals("construct")) {
				// 在内存中建立orderBench.txt的索引 建立期间可随时调用write将某个块写出去

				List<String> buyerfiles = new ArrayList<String>();
				buyerfiles.add("prerun_data/buyer.0.0");
				buyerfiles.add("prerun_data/buyer.1.1");
				//buyerfiles.add("benchmark/buyer_records.txt");
				//buyerfiles.add("benchmark/buyer_records_2.txt");
				//buyerfiles.add("benchmark/buyer_records_3.txt");
				//buyerfiles.add("benchmark/buyer_records_4.txt");
				//buyerfiles.add("benchmark/buyer_records.txt");
				//buyerfiles.add("benchmark/buyer_records_1.txt");

				List<String> goodfiles = new ArrayList<String>();
				goodfiles.add("prerun_data/good.0.0");
				goodfiles.add("prerun_data/good.1.1");
				goodfiles.add("prerun_data/good.2.2");
				//goodfiles.add("benchmark/good_records.txt");
				//goodfiles.add("benchmark/good_records_2.txt");
				//goodfiles.add("benchmark/good_records_3.txt");
				//goodfiles.add("benchmark/good_records_4.txt");
				//goodfiles.add("benchmark/good_records.txt");
				//goodfiles.add("benchmark/good_records_1.txt");

				List<String> orderfiles = new ArrayList<String>();
				orderfiles.add("prerun_data/order.0.0");
				orderfiles.add("prerun_data/order.0.3");
				orderfiles.add("prerun_data/order.1.1");
				orderfiles.add("prerun_data/order.2.2");
				//orderfiles.add("benchmark/order_records.txt");
				//orderfiles.add("benchmark/order_records_2.txt");
				//orderfiles.add("benchmark/order_records_3.txt");
				//orderfiles.add("benchmark/order_records_4.txt");
				//orderfiles.add("benchmark/order_records_1.txt");
				//orderfiles.add("benchmark/order_records.txt");

				List<String> storeFolders = new ArrayList<String>();
				// 添加三个盘符
				storeFolders.add("folder1/");
				storeFolders.add("folder2/");
				storeFolders.add("folder3/");

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
				orderSystem.queryOrder( Long.valueOf(rawCommand[0]), null));
				
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
				
				
			} else if (command.startsWith("lookup3")) {
				// lookup:xxx 查找某个key值的value
				String[] rawCommand = command.substring(command.indexOf(":") + 1).split(",");
				String goodId = rawCommand[0];
				List<String> keys = new ArrayList<String>();
				for( int i = 1; i < rawCommand.length; i++ ) {
					keys.add(rawCommand[i]);
				}
				Iterator<Result> results = orderSystem.queryOrdersBySaler("", goodId, keys);
				while(results.hasNext()) {
					System.out.println("values:" + results.next());
				}
				
			} else if (command.startsWith("lookup4")) {
				// lookup:xxx 查找某个key值的value
				String[] rawCommand = command.substring(command.indexOf(":") + 1).split(",");
				String goodId = rawCommand[0];
				String key = rawCommand[1];
				System.out.println(orderSystem.sumOrdersByGood(goodId, key));
				
			} else if (command.equals("quit")) {
				// 索引使用完毕 退出
				
			}
		}

		scanner.close();

	}

	public OrderSystemImpl() {
		// 初始化操作

		// 存订单表里的orderId索引<文件名（尽量短名）,内存里缓存的索引DiskHashTable>
		orderIdIndexList = new ConcurrentHashMap<Integer, DiskHashTable<Long, byte[]>>();
		// 订单表里的buyerId代理键索引
		orderBuyerIdIndexList = new ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>>();
		// 订单表里的goodId代理键索引
		orderGoodIdIndexList = new ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>>();
		// 订单表里的可计算字段索引Map
		//orderCountableIndexList = new ConcurrentHashMap<Integer, List<DiskHashTable<Integer, List<byte[]>>>>();
		// buyerId里的buyerId代理键索引
		buyerIdIndexList = new ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>>();
		// goodId里的goodId代理键索引
		goodIdIndexList = new ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>>();
		orderHandlersList = new ConcurrentHashMap<Integer,LinkedBlockingQueue<RandomAccessFile>>();
		buyerHandlersList = new ConcurrentHashMap<Integer,LinkedBlockingQueue<RandomAccessFile>>();
		goodHandlersList = new ConcurrentHashMap<Integer,LinkedBlockingQueue<RandomAccessFile>>();
		
		orderFileMapping = new DataFileMapping();
		buyerFileMapping = new DataFileMapping();
		goodFileMapping = new DataFileMapping();

		orderAttrList = new HashSet<String>(); // 保存order表的所有字段名称
		buyerAttrList = new HashSet<String>(); // 保存buyer表的所有字段名称
		goodAttrList = new HashSet<String>(); // 保存good表的所有字段名称
		//buyerIdSurrKeyFile = new FilePathWithIndex(); // 存代理键索引块的文件地址和索引元数据偏移地址
		//goodIdSurrKeyFile = new FilePathWithIndex();

//		JVMMonitorThread jvmMonitorThread = new JVMMonitorThread("JVMMonitor", BucketCachePool.getInstance());
		CacheMonitorThread cacheMonitorThread = new CacheMonitorThread(ConcurrentCache.getInstance());
		//threadPool.addMonitor(jvmMonitorThread);
		threadPool.addMonitor(cacheMonitorThread);
		threadPool.startMonitors();
		rowCache = SimpleCache.getInstance();
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
		
		for (int filePathIndex : orderFileMapping.getAllFileIndexs()) {
			DiskHashTable<Long, byte[]> hashTable = orderIdIndexList.get(filePathIndex);
			List<byte[]> results = hashTable.get(orderid);
			
			if (results.size() != 0) {
				// find the records offset
				for( byte[] encodedOffset : results) {
					// 解码获得long型的offset
					FileIndexWithOffset fileInfo= RecordsUtils.decodeIndex(encodedOffset);
					long offset = fileInfo.offset;
					Row temp = rowCache.getFromCache(encodedOffset, TableName.OrderTable);
					if(temp != null) {
						temp = temp.getKV(RaceConfig.orderId).valueAsLong() == orderid ?
								temp : Row.createKVMapFromLine(RecordsUtils.getStringFromFile(
										orderHandlersList.get(filePathIndex), offset, 
										TableName.OrderTable));
					}
					else {
						// 从文件里读数据
						String diskValue = RecordsUtils.getStringFromFile(
								orderHandlersList.get(filePathIndex), offset, TableName.OrderTable);
						temp = Row.createKVMapFromLine( diskValue );
						// 放入缓冲区
						rowCache.putInCache(encodedOffset, diskValue , TableName.OrderTable);
					}
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
	public Row getRowById(TableName tableName, Object id) throws TypeException {
		Row result = null;
		String idString = String.valueOf(id);
		switch (tableName) {
		case OrderTable:
			for (int fileIndex : orderFileMapping.getAllFileIndexs()) {
				long orderid = Long.valueOf(idString);
				DiskHashTable<Long, byte[]> hashTable = orderIdIndexList.get(fileIndex);
				List<byte[]> results = hashTable.get(orderid);
				
				if (results.size() != 0) {
					// find the records offset
					for( byte[] encodedOffset : results) {
						//解码byte数组
						FileIndexWithOffset offsetInfo = RecordsUtils.decodeIndex(encodedOffset);
						long offset = offsetInfo.offset;
						Row temp = rowCache.getFromCache(encodedOffset, tableName);
						if(temp != null) {
							temp = temp.getKV(RaceConfig.orderId).valueAsLong() == orderid ?
									temp : Row.createKVMapFromLine(RecordsUtils.getStringFromFile(
											orderHandlersList.get(fileIndex), offset, tableName));
						}
						else {
							String diskValue = RecordsUtils.getStringFromFile(
									orderHandlersList.get(fileIndex),offset, tableName);
							temp = Row.createKVMapFromLine(diskValue);
							rowCache.putInCache(encodedOffset, diskValue, tableName);
						}
						result = temp;
						break;
					}
					break;
				}
			}	
			break;
		case BuyerTable:
			// 将事实键转为代理键
			Integer surrId = String.valueOf(id).hashCode();
			// 先在缓冲区里找
			result = rowCache.getFromCache(surrId, TableName.BuyerTable);
			if( result != null) {
				// 在缓冲区找到了
				return result;
			}
			else {
				// 在索引里找
				for (int filePathIndex : buyerFileMapping.getAllFileIndexs()) {
					DiskHashTable<Integer, List<byte[]>> hashTable = buyerIdIndexList
							.get(filePathIndex);
					List<byte[]> results = hashTable.get(surrId);
					if (results.size() != 0) {
						for( byte[] encodedOffset : results) {
							long offset = RecordsUtils.decodeIndex(encodedOffset).offset;
							String records = RecordsUtils.getStringFromFile(
									buyerHandlersList.get(filePathIndex), offset, tableName);
							Row temp = Row.createKVMapFromLine(records);
							if( temp.getKV(RaceConfig.buyerId).valueAsString().equals(id)) {
								result = temp;
								// 放入缓冲区
								rowCache.putInCache(surrId, records, tableName);
								break;
							}
						}
						break;
					}

				}
			}
			break;
		case GoodTable:
			Integer goodSurrId = String.valueOf(id).hashCode();
			// 先在缓冲区里找
			result = rowCache.getFromCache(goodSurrId, tableName);
			if( result != null) {
				return result;
			}
			else {
				for (int filePathIndex : goodFileMapping.getAllFileIndexs()) {
					DiskHashTable<Integer, List<byte[]>> hashTable = goodIdIndexList.get(filePathIndex);
					List<byte[]> results = hashTable.get(goodSurrId);
					if (results.size() != 0) {
						for( byte[] encodedOffset : results) {
							long offset = RecordsUtils.decodeIndex(encodedOffset).offset;
							String records = RecordsUtils.getStringFromFile(
									goodHandlersList.get(filePathIndex), offset, tableName);
							Row temp = Row.createKVMapFromLine(records);
							if( temp.getKV(RaceConfig.goodId).valueAsString().equals(id)) {
								result = temp;
								// 放入缓冲区
								rowCache.putInCache(goodSurrId, records, tableName);
								break;
							}
						}
						break;
					}

				}
			}
			break;
		}
		
		return result;
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
            QueryOrderThread t = new QueryOrderThread(this,orderId, keys);
            Future<ResultImpl> future = queryExe.submit(t);
            try {
                result = future.get();
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
            QueryOrderByBuyerThread t = new QueryOrderByBuyerThread(this, startTime, endTime, buyerid);
            Future<Iterator<Result>> future = queryExe.submit(t);
            try {
                iterator = future.get();
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
            QueryOrdersBySalerThread t = new QueryOrdersBySalerThread(this,salerid, goodid, keys);
            Future<Iterator<Result>> future = queryExe.submit(t);
            try {
                iterator = future.get();
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
            SumOrdersByGoodThread t = new SumOrdersByGoodThread(this,goodid, key);
            Future<KeyValueImpl> future = queryExe.submit(t);
            try {
                result = future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
		return result;
	}

}
