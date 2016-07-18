package com.alibaba.middleware.race;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.handlefile.ConstructSystem;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.threads.*;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

/**
 * 订单系统实现
 * 
 * @author hankwing
 *
 */
public class OrderSystemImpl implements OrderSystem {

	// 存订单表里的orderId索引<文件名（尽量短名）,内存里缓存的索引DiskHashTable>
	public ConcurrentHashMap<String, DiskHashTable<Long, Long>> orderIdIndexList = null;
	// 订单表里的buyerId代理键索引
	public ConcurrentHashMap<String, DiskHashTable<Long, List<Long>>> orderBuyerIdIndexList = null;
	// 订单表里的goodId代理键索引
	public ConcurrentHashMap<String, DiskHashTable<Long, List<Long>>> orderGoodIdIndexList = null;
	// 订单表里的可计算字段索引Map
	public ConcurrentHashMap<String, List<DiskHashTable<Long, List<Long>>>> orderCountableIndexList = null;
	// buyerId里的buyerId代理键索引
	public ConcurrentHashMap<String, DiskHashTable<Long, Long>> buyerIdIndexList = null;
	// goodId里的goodId代理键索引
	public ConcurrentHashMap<String, DiskHashTable<Long, Long>> goodIdIndexList = null;

	public List<FilePathWithIndex> orderFileList = null; // 保存order表所有文件的名字
	public List<FilePathWithIndex> buyerFileList = null; // 保存buyer表所有文件的名字
	public List<FilePathWithIndex> goodFileList = null; // 保存good表所有文件的名字

	public HashSet<String> orderAttrList = null; // 保存order表的所有字段名称
	public HashSet<String> buyerAttrList = null; // 保存buyer表的所有字段名称
	public HashSet<String> goodAttrList = null; // 保存good表的所有字段名称

	public FilePathWithIndex buyerIdSurrKeyFile = null; // 存代理键索引块的文件地址和索引元数据偏移地址
	public FilePathWithIndex goodIdSurrKeyFile = null; // 存代理键索引块的文件地址和索引元数据偏移地址
	public DiskHashTable<String, Long> buyerIdSurrKeyIndex = null; // 缓存buyerId事实键与代理键
	public DiskHashTable<String, Long> goodIdSurrKeyIndex = null; // 缓存goodId事实键与代理键

    private ExecutorService queryExe = ThreadPool.getInstance().getQueryExe();

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
				buyerfiles.add("benchmark/buyer_records.txt");

				List<String> goodfiles = new ArrayList<String>();
				goodfiles.add("benchmark/good_records.txt");

				List<String> orderfiles = new ArrayList<String>();
				orderfiles.add("benchmark/order_records.txt");

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
				orderSystem.queryOrder( Long.valueOf(rawCommand[0]), keys));
				
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
				
			}else if (command.equals("quit")) {
				// 索引使用完毕 退出
				
			}
		}

		scanner.close();

	}

	public OrderSystemImpl() {
		// 初始化操作

		// 存订单表里的orderId索引<文件名（尽量短名）,内存里缓存的索引DiskHashTable>
		orderIdIndexList = new ConcurrentHashMap<String, DiskHashTable<Long, Long>>();
		// 订单表里的buyerId代理键索引
		orderBuyerIdIndexList = new ConcurrentHashMap<String, DiskHashTable<Long, List<Long>>>();
		// 订单表里的goodId代理键索引
		orderGoodIdIndexList = new ConcurrentHashMap<String, DiskHashTable<Long, List<Long>>>();
		// 订单表里的可计算字段索引Map
		orderCountableIndexList = new ConcurrentHashMap<String, List<DiskHashTable<Long, List<Long>>>>();
		// buyerId里的buyerId代理键索引
		buyerIdIndexList = new ConcurrentHashMap<String, DiskHashTable<Long, Long>>();
		// goodId里的goodId代理键索引
		goodIdIndexList = new ConcurrentHashMap<String, DiskHashTable<Long, Long>>();

		orderFileList = new ArrayList<FilePathWithIndex>(); // 保存order表所有文件的名字
		buyerFileList = new ArrayList<FilePathWithIndex>(); // 保存buyer表所有文件的名字
		goodFileList = new ArrayList<FilePathWithIndex>(); // 保存good表所有文件的名字

		orderAttrList = new HashSet<String>(); // 保存order表的所有字段名称
		buyerAttrList = new HashSet<String>(); // 保存buyer表的所有字段名称
		goodAttrList = new HashSet<String>(); // 保存good表的所有字段名称
		buyerIdSurrKeyFile = new FilePathWithIndex(); // 存代理键索引块的文件地址和索引元数据偏移地址
		goodIdSurrKeyFile = new FilePathWithIndex();

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
		
		buyerIdSurrKeyIndex = new DiskHashTable<String,Long>(
				RaceConfig.storeFolders[0] + 
				RaceConfig.buyerSurrFileName,
				RaceConfig.storeFolders[0] +
				RaceConfig.buyerSurrFileName, Long.class);
		
		goodIdSurrKeyIndex = new DiskHashTable<String,Long>(
				RaceConfig.storeFolders[0] +
				RaceConfig.goodSurrFileName,
				RaceConfig.storeFolders[0] +
				RaceConfig.goodSurrFileName, Long.class);
		long startTime = System.currentTimeMillis();

		ConstructSystem constructSystem = new ConstructSystem(orderIdIndexList,
				orderBuyerIdIndexList, orderGoodIdIndexList,
				orderCountableIndexList, orderFileList, buyerFileList,
				goodFileList, orderAttrList, buyerAttrList, goodAttrList,
				buyerIdSurrKeyFile, goodIdSurrKeyFile, buyerIdIndexList,
				goodIdIndexList, buyerIdSurrKeyIndex, goodIdSurrKeyIndex);
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
	@SuppressWarnings("unchecked")
	public long getSurrogateKey(String id, IdName idName) {
		long surrogateKey = 0;
		switch (idName) {
		case OrderId:
			break;
		case BuyerId:
			if (buyerIdSurrKeyIndex == null) {
				buyerIdSurrKeyIndex = getHashDiskTable(
						buyerIdSurrKeyFile.getFilePath(),
						buyerIdSurrKeyFile.getSurrogateIndex());
			}
			surrogateKey = buyerIdSurrKeyIndex.get(id).get(0);
			break;
		case GoodId:
			if (goodIdSurrKeyIndex == null) {
				goodIdSurrKeyIndex = getHashDiskTable(
						goodIdSurrKeyFile.getFilePath(),
						goodIdSurrKeyFile.getSurrogateIndex());
			}
			surrogateKey = goodIdSurrKeyIndex.get(id).get(0);
			break;
		}

		return surrogateKey;
	}

	/**
	 * 根据orderid查找索引返回记录 无记录则返回null
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Row getRowById(TableName tableName, IdName idName, Object id,
			Collection<String> keys) {
		Row result = null;
		try {
			switch (tableName) {
			case OrderTable:
				for (FilePathWithIndex filePath : orderFileList) {
					DiskHashTable<Long, Long> hashTable = orderIdIndexList
							.get(filePath.getFilePath());
					if (hashTable == null) {
						FileInputStream streamIn = new FileInputStream(
								filePath.getFilePath());
						switch (idName) {
						case OrderId:
							streamIn.getChannel().position(
									filePath.getOrderIdIndex());
							break;
						case BuyerId:
							// 这里要将id转化成代理键
							id = getSurrogateKey(String.valueOf(id), idName);
							streamIn.getChannel().position(
									filePath.getOrderBuyerIdIndex());
							break;
						case GoodId:
							// 这里要将id转化成代理键
							id = getSurrogateKey(String.valueOf(id), idName);
							streamIn.getChannel().position(
									filePath.getOrderGoodIdIndex());
							break;
						}

						ObjectInputStream objectinputstream = new ObjectInputStream(
								streamIn);

						hashTable = (DiskHashTable<Long, Long>) objectinputstream
								.readObject();
						hashTable.restore();
						objectinputstream.close();
					}
					if (hashTable.get(id).size() != 0) {
						// find the records offset
						// 不管key是什么，都得载入固定order表里的固定key
						/*System.out.println("records offset:"
								+ hashTable.get(id).get(0));*/
						orderIdIndexList.put(filePath.getFilePath(), hashTable);
						result = RecordsUtils.getRecordsByKeysFromFile(
								filePath.getFilePath(), keys, hashTable.get(id).get(0));
						break; // 找到了就退出 因为orderId不会重复
					}

				}
				break;
			case BuyerTable:
				// 将事实键转为代理键
				id = getSurrogateKey(String.valueOf(id), idName);
				for (FilePathWithIndex filePath : buyerFileList) {
					DiskHashTable<Long, Long> hashTable = buyerIdIndexList
							.get(filePath.getFilePath());
					if (hashTable == null) {
						buyerIdSurrKeyIndex = getHashDiskTable(
								filePath.getFilePath(),
								filePath.getBuyerIdIndex());
					}
					if (hashTable.get(id).size() != 0) {
						// find the records offset
						/*System.out.println("records offset:"
								+ hashTable.get(id).size());*/
						buyerIdIndexList.put(filePath.getFilePath(), hashTable);
						result = RecordsUtils.getRecordsByKeysFromFile(
								filePath.getFilePath(), keys, hashTable.get(id).get(0));
						break; // 找到了就退出 因为buyerId不会重复
					}

				}
				break;
			case GoodTable:
				id = getSurrogateKey(String.valueOf(id), idName);
				for (FilePathWithIndex filePath : goodFileList) {
					DiskHashTable<Long, Long> hashTable = goodIdIndexList
							.get(filePath.getFilePath());
					if (hashTable == null) {
						buyerIdSurrKeyIndex = getHashDiskTable(
								filePath.getFilePath(),
								filePath.getGoodIdIndex());
					}
					if (hashTable.get(id).size() != 0) {
						// find the records offset
						/*System.out.println("records offset:"
								+ hashTable.get(id).size());*/
						goodIdIndexList.put(filePath.getFilePath(), hashTable);
						result = RecordsUtils.getRecordsByKeysFromFile(
								filePath.getFilePath(), keys, hashTable.get(id).get(0));
						break; // 找到了就退出 因为buyerId不会重复
					}

				}
				break;
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
            SumOrdersByGoodThread t = new SumOrdersByGoodThread(goodid, key);
            Future<KeyValueImpl> future = queryExe.submit(t);
            try {
                result = future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

//		long surrId = getSurrogateKey(goodid, IdName.GoodId);
//		for (FilePathWithIndex filePath : orderFileList) {
//			DiskHashTable<Long, List<Long>> hashTable = orderBuyerIdIndexList
//					.get(filePath.getFilePath());
//			if (hashTable == null) {
//				hashTable = getHashDiskTable(filePath.getFilePath(),
//						filePath.getBuyerIdIndex());
//			}
//			if (hashTable.get(surrId).size() != 0) {
//				// find the records offset
//				// 找到后，按照降序插入TreeMap中
//				System.out.println("records offset:"
//						+ hashTable.get(surrId).size());
//				orderBuyerIdIndexList.put(filePath.getFilePath(), hashTable);
//			}
//
//		}

		return result;
	}

}
