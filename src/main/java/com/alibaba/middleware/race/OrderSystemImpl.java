package com.alibaba.middleware.race;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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
	public ConcurrentHashMap<String,List<DiskHashTable<Long, List<Long>>>> 
	orderCountableIndexList = null;								
	// buyerId里的buyerId代理键索引
	public ConcurrentHashMap<String, DiskHashTable<Long, Long>> buyerIdIndexList = null;
	// goodId里的goodId代理键索引
	public ConcurrentHashMap<String, DiskHashTable<Long, Long>> goodIdIndexList = null;				
	
	public List<FilePathWithIndex> orderFileList = null;					// 保存order表所有文件的名字
	public List<FilePathWithIndex> buyerFileList = null;					// 保存buyer表所有文件的名字
	public List<FilePathWithIndex> goodFileList = null;						// 保存good表所有文件的名字
	
	public List<String> orderAttrList = null;					// 保存order表的所有字段名称
	public List<String> buyerAttrList = null;					// 保存buyer表的所有字段名称
	public List<String> goodAttrList = null;						// 保存good表的所有字段名称
	
	public FilePathWithIndex buyerIdSurrKeyFile = null;			// 存代理键索引块的文件地址和索引元数据偏移地址
	public FilePathWithIndex goodIdSurrKeyFile = null;			// 存代理键索引块的文件地址和索引元数据偏移地址
	public DiskHashTable<String, Long> buyerIdSurrKeyIndex = null;			// 缓存buyerId事实键与代理键
	public DiskHashTable<String, Long> goodIdSurrKeyIndex = null;			// 缓存goodId事实键与代理键

    private ExecutorService queryExe = ThreadPool.getInstance().getQueryExe();
	
	public static void main(String[] args) {

		OrderSystemImpl orderSystem = new OrderSystemImpl();
		
		List<String> buyerfiles = new ArrayList<String>();
		buyerfiles.add("benchmark\\buyer_records.txt");
		buyerfiles.add("buyer_records_1.txt");
		buyerfiles.add("buyer_records_2.txt");
		
		List<String> goodfiles = new ArrayList<String>();
		goodfiles.add("benchmark\\good_records.txt");
		goodfiles.add("good_records_1.txt");
		goodfiles.add("good_records_2.txt");
		
		List<String> orderfiles = new ArrayList<String>();
		orderfiles.add("order_records.txt");
		
		List<String> storeFolders = new ArrayList<String>();
		storeFolders.add("benchmark//");
		
		try {
			orderSystem.construct(orderfiles, buyerfiles, goodfiles, storeFolders);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	public OrderSystemImpl() {
		// 初始化操作
		
		//orderIdIndexList = new ArrayList<DiskHashTable<Long, Long>>();
		//orderBuyerIdIndexList = new ArrayList<DiskHashTable<Long, List<Long>>>();
		//orderGoodIdIndexList = new ArrayList<DiskHashTable<Long, List<Long>>>();
		//orderCountableIndexList = new HashMap<String,List<DiskHashTable<Long, List<Long>>>>();
		
		//buyeridIndexList = new ArrayList<DiskHashTable<Long, Long>>();
		//goodIdIndexList = new ArrayList<DiskHashTable<Long, Long>>();
	}

	/**
	 * 
	 * 读每种类型的文件  直接写到小文件里  处理逻辑由单独线程处理
	 *
	 */
	public void construct(Collection<String> orderFiles,
			Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException,
			InterruptedException {
		// 将存储目录存起来 之后建小文件及索引文件的时候用
		RaceConfig.storeFolders = (String[]) storeFolders.toArray();
		
		long startTime = System.currentTimeMillis();
		
		ConstructSystem constructSystem = new ConstructSystem(orderIdIndexList, orderBuyerIdIndexList,
				orderGoodIdIndexList, orderCountableIndexList,
				orderFileList, buyerFileList, goodFileList, orderAttrList, buyerAttrList, 
				goodAttrList, buyerIdSurrKeyFile, goodIdSurrKeyFile,
				buyerIdIndexList, goodIdIndexList,buyerIdSurrKeyIndex,goodIdSurrKeyIndex);
		constructSystem.startHandling(buyerFiles, goodFiles, orderFiles, storeFolders, 1);
		
		long endTime = System.currentTimeMillis();
		System.out.println("construct time:" + (endTime - startTime) / 1000);
	}
	
	public DiskHashTable getHashDiskTable( String filePath, long offSet) {
		DiskHashTable hashTable = null;
		FileInputStream streamIn;
		try {
			streamIn = new FileInputStream(filePath);
			streamIn.getChannel().position(offSet);		
			ObjectInputStream objectinputstream = new ObjectInputStream(streamIn);
		    
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
	 * @param id
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public long getSurrogateKey( Object id, IdName idName) {
		long surrogateKey = 0;
		switch( idName) {
		case OrderId:
			break;
		case BuyerId:
			if( buyerIdSurrKeyIndex == null) {
				buyerIdSurrKeyIndex = getHashDiskTable(buyerIdSurrKeyFile.getFilePath(),
						buyerIdSurrKeyFile.getSurrogateIndex());
			}
			surrogateKey = buyerIdSurrKeyIndex.get(id).get(0);
			break;
		case GoodId:
			if( goodIdSurrKeyIndex == null) {
				goodIdSurrKeyIndex = getHashDiskTable(goodIdSurrKeyFile.getFilePath(),
						goodIdSurrKeyFile.getSurrogateIndex());
			}
			surrogateKey = goodIdSurrKeyIndex.get(id).get(0);
			break;
		}
		
		return surrogateKey;
	}

	/**
	 * 根据orderid查找索引返回记录 无记录则返回null
	 * @param tableName
     * @param id
     * @param idName
     * @param keys
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Row getRowById( TableName tableName, IdName idName, Object id, Collection<String> keys) {
		Row result = new Row();
		try {
			switch( tableName) {
			case OrderTable:
				for( FilePathWithIndex filePath : orderFileList) {
					DiskHashTable<Long, Long> hashTable = orderIdIndexList.get(filePath.getFilePath());
					if( hashTable == null) {
						FileInputStream streamIn = new FileInputStream(filePath.getFilePath());
						switch( idName) {
						case OrderId:
							streamIn.getChannel().position(filePath.getOrderIdIndex());
							break;
						case BuyerId:
							// 这里要将id转化成代理键
							id = getSurrogateKey(id, idName);
							streamIn.getChannel().position(filePath.getOrderBuyerIdIndex());
							break;
						case GoodId:
							// 这里要将id转化成代理键
							id = getSurrogateKey(id, idName);
							streamIn.getChannel().position(filePath.getOrderGoodIdIndex());
							break;
						}

						ObjectInputStream objectinputstream = new ObjectInputStream(streamIn);

						hashTable = (DiskHashTable<Long, Long>) objectinputstream.readObject();
						hashTable.restore();
						objectinputstream.close();
					}
				    if( hashTable.get(id).size() != 0) {
				    	// find the records offset
				    	// 不管key是什么，都得载入固定order表里的固定key
				    	System.out.println("records offset:" + hashTable.get(id).size());
				    	orderIdIndexList.put(filePath.getFilePath(), hashTable);
				    	if( idName == IdName.GoodId) {
				    		break;		// 找到了就退出 因为orderId不会重复
				    	}
				    }

				}
				break;
			case BuyerTable:
				// 将事实键转为代理键
				if( keys.size() == 0) {
					return result;
				}
				id = getSurrogateKey(id, idName);
				for( FilePathWithIndex filePath : buyerFileList) {
					DiskHashTable<Long, Long> hashTable =
							buyerIdIndexList.get(filePath.getFilePath());
					if( hashTable == null) {
						buyerIdSurrKeyIndex = getHashDiskTable(filePath.getFilePath(),
								filePath.getBuyerIdIndex());
					}
				    if( hashTable.get(id).size() != 0) {
				    	// find the records offset
				    	System.out.println("records offset:" + hashTable.get(id).size());
				    	buyerIdIndexList.put(filePath.getFilePath(), hashTable);
				    	break;		// 找到了就退出 因为buyerId不会重复
				    }

				}
				break;
			case GoodTable:
				if( keys.size() == 0) {
					return result;
				}
				id = getSurrogateKey(id, idName);
				for( FilePathWithIndex filePath : goodFileList) {
					DiskHashTable<Long, Long> hashTable =
							goodIdIndexList.get(filePath.getFilePath());
					if( hashTable == null) {
						buyerIdSurrKeyIndex = getHashDiskTable(filePath.getFilePath(),
								filePath.getGoodIdIndex());
					}
				    if( hashTable.get(id).size() != 0) {
				    	// find the records offset
				    	System.out.println("records offset:" + hashTable.get(id).size());
				    	goodIdIndexList.put(filePath.getFilePath(), hashTable);
				    	break;		// 找到了就退出 因为buyerId不会重复
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
	   *          订单号
	   * @param keys
	   *          待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
	   * @return 查询结果，如果该订单不存在，返回null
	*/
	public Result queryOrder(long orderId, Collection<String> keys) {
		// 主要思想：先判断keys在哪个表里  之后根据索引在不同表里找不同字段
		ResultImpl result = null;

        if (!(queryExe == null)) {
            QueryOrderThread queryOrderThread = new QueryOrderThread(orderId, keys);
            Future<ResultImpl> future = queryExe.submit(queryOrderThread);
            try {
                result = future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
//		Row resultKV = new Row();
//		resultKV.putKV(RaceConfig.orderId, orderId);
//		result = new ResultImpl(orderId, resultKV);
//		if( keys == null) {
//			// 为Null 查询所有字段
//			resultKV.putAll(getRowById( TableName.OrderTable, IdName.OrderId, orderId, keys));
//			resultKV.putAll(getRowById(
//					TableName.BuyerTable, IdName.BuyerId, resultKV.get(RaceConfig.buyerId), keys));
//			resultKV.putAll(getRowById(
//					TableName.GoodTable, IdName.GoodId, resultKV.get(RaceConfig.goodId), keys));
//		}
//		else if( keys.isEmpty()) {
//			// 为空 排除所有字段
//			result = new ResultImpl( orderId, resultKV );
//		}
//		else {
//			// 查询指定字段
//			List<String> orderKeys = new ArrayList<String>();
//			List<String> buyerKesy = new ArrayList<String>();
//			List<String> goodKeys = new ArrayList<String>();
//			for(String key : keys) {
//				if( orderAttrList.contains(key)) {
//					orderKeys.add(key);
//				}
//				else if( orderAttrList.contains(key)) {
//					orderKeys.add(key);
//				}
//				else if( goodAttrList.contains(key)) {
//					goodKeys.add(key);
//				}
//			}
//
//			resultKV.putAll(getRowById( TableName.OrderTable, IdName.OrderId, orderId, orderKeys));
//			resultKV.putAll(getRowById(
//					TableName.BuyerTable, IdName.BuyerId, resultKV.get(RaceConfig.buyerId), buyerKesy));
//			resultKV.putAll(getRowById(
//					TableName.GoodTable, IdName.GoodId, resultKV.get(RaceConfig.goodId), goodKeys));
//
//		}
		
		return result;
	}

	/**
	   * 查询某位买家createtime字段从[startTime, endTime) 时间范围内发生的所有订单的所有信息
	   * 
	   * @param startTime 订单创建时间的下界
	   * @param endTime 订单创建时间的上界
	   * @param buyerid
	   *          买家Id
	   * @return 符合条件的订单集合，按照createtime大到小排列
	*/
	@SuppressWarnings("unchecked")
	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
			String buyerid) {
        Iterator<Result> resultIterator = null;

        if (!(queryExe == null)) {
            QueryOrderByBuyerThread queryOrderByBuyerThread = new QueryOrderByBuyerThread(startTime, endTime, buyerid);
            Future<Iterator<Result>> future = queryExe.submit(queryOrderByBuyerThread);
            try {
                resultIterator = future.get();
            } catch (InterruptedException e) {
                // TODO
                e.printStackTrace();
            } catch (ExecutionException e) {
                // TODO
                e.printStackTrace();
            }
        }

        return resultIterator;
		// 根据买家ID在索引里找到结果 再判断结果是否介于startTime和endTime之间  结果集合按照createTime插入排序
//		TreeMap<Long,Result> results = new TreeMap<Long, Result>(Collections.reverseOrder());
//		long surrId = getSurrogateKey(buyerid, IdName.BuyerId);
//		for( FilePathWithIndex filePath : buyerFileList) {
//			DiskHashTable<Long, Long> hashTable =
//					buyerIdIndexList.get(filePath.getFilePath());
//			if( hashTable == null) {
//				hashTable = getHashDiskTable(filePath.getFilePath(),
//						filePath.getBuyerIdIndex());
//			}
//		    if( hashTable.get(surrId).size() != 0) {
//		    	// find the records offset
//		    	// 找到后，按照降序插入TreeMap中
//		    	System.out.println("records offset:" + hashTable.get(surrId).size());
//		    	buyerIdIndexList.put(filePath.getFilePath(), hashTable);
//		    }
//
//		}
//		return results.values().iterator();
	}

	/**
	   * 查询某位卖家某件商品所有订单的某些字段
	   * 
	   * @param salerid 卖家Id
	   * @param goodid 商品Id
	   * @param keys 待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
	   * @return 符合条件的订单集合，按照订单id从小至大排序
	*/
	@SuppressWarnings("unchecked")
	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
			Collection<String> keys) {
        Iterator<Result> resultIterator = null;

        if (!(queryExe == null)) {
            QueryOrdersBySalerThread queryOrdersBySalerThread = new QueryOrdersBySalerThread(salerid, goodid, keys);
            Future<Iterator<Result>> future = queryExe.submit(queryOrdersBySalerThread);
            try {
                resultIterator = future.get();
            } catch (InterruptedException e) {
                // TODO
                e.printStackTrace();
            } catch (ExecutionException e) {
                // TODO
                e.printStackTrace();
            }
        }

        return resultIterator;

		// 根据商品ID找到多条订单信息  再筛选出keys 结果集按照订单id插入排序
//		TreeMap<Long,Result> results = new TreeMap<Long, Result>(Collections.reverseOrder());
//		long surrId = getSurrogateKey(goodid, IdName.GoodId);
//		for( FilePathWithIndex filePath : orderFileList) {
//			DiskHashTable<Long, List<Long>> hashTable =
//					orderBuyerIdIndexList.get(filePath.getFilePath());
//			if( hashTable == null) {
//				hashTable = getHashDiskTable(filePath.getFilePath(),
//						filePath.getBuyerIdIndex());
//			}
//		    if( hashTable.get(surrId).size() != 0) {
//		    	// find the records offset
//		    	// 找到后，按照降序插入TreeMap中
//		    	System.out.println("records offset:" + hashTable.get(surrId).size());
//		    	orderBuyerIdIndexList.put(filePath.getFilePath(), hashTable);
//		    }
//
//		}
//		return results.values().iterator();
	}

	/**
	   * 对某件商品的某个字段求和，只允许对long和double类型的KV求和 如果字段中既有long又有double，则使用double
	   * 如果求和的key中包含非long/double类型字段，则返回null 如果查询订单中的所有商品均不包含该字段，则返回null
	   * 
	   * @param goodid 商品Id
	   * @param key 求和字段
	   * @return 求和结果
	*/
	@SuppressWarnings("unchecked")
	public KeyValue sumOrdersByGood(String goodid, String key) {
		// 根据商品ID找到多条订单信息  再根据key值加和
		KeyValueImpl result = null;

        if (!(queryExe == null)) {
            SumOrdersByGoodThread sumOrdersByGoodThread = new SumOrdersByGoodThread(goodid, key);
            Future<KeyValueImpl> future = queryExe.submit(sumOrdersByGoodThread);
            try {
                result = future.get();
            } catch (InterruptedException e) {
                // TODO
                e.printStackTrace();
            } catch (ExecutionException e) {
                // TODO
                e.printStackTrace();
            }
        }

        return result;
//		long surrId = getSurrogateKey(goodid, IdName.GoodId);
//		for( FilePathWithIndex filePath : orderFileList) {
//			DiskHashTable<Long, List<Long>> hashTable =
//					orderBuyerIdIndexList.get(filePath.getFilePath());
//			if( hashTable == null) {
//				hashTable = getHashDiskTable(filePath.getFilePath(),
//						filePath.getBuyerIdIndex());
//			}
//		    if( hashTable.get(surrId).size() != 0) {
//		    	// find the records offset
//		    	// 找到后，按照降序插入TreeMap中
//		    	System.out.println("records offset:" + hashTable.get(surrId).size());
//		    	orderBuyerIdIndexList.put(filePath.getFilePath(), hashTable);
//		    }
//
//		}
//		return null;
	}
}
