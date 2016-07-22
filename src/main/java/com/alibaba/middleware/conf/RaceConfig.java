package com.alibaba.middleware.conf;

import java.io.Serializable;

/**
 * 将可选的配置信息写到这个文件里
 * @author hankwing
 *
 */
public class RaceConfig implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4468293265402134589L;
	
	public static int handleThreadNumber = 3;				// 读写线程数
	public static int QueueNumber = 10000;					// 建索引时的一个缓冲队列的大小
	public static int fileHandleNumber = 8;				// 每个文件预先打开的句柄数
	
	public static int cacheNumberOneRead = 100;					// 每读一次就放入缓冲区中的行的数量
	public static int bucketNumberOneRead = 50;					// 每读一次桶就放入缓冲区中的桶的数量
	public static long smallFileCapacity = 3000000;
	/**
	 * Thread pool config
	 * */
	public static int rowCacheNumber = 10000000;					// 在内存里最多保留几个row数据
	public static int monitorThreadNum = 1;
    public static int workerThreadNum = Runtime.getRuntime().availableProcessors() * 2;
	public static int queryThreadNum = Runtime.getRuntime().availableProcessors() * 2;
	// JVMMonitorThread
    public static int monitorInitDelayInMills = 20 *1000;			// 20s后开始检测内存
    public static int monitorFixedDelayInMills = 10 * 1000;			// 每10s检测一次内存
	public static float memFactor = 0.9f;
	public static int gcCounterThreshold = 2;
	public static int removeBucketNum = 100;

	/**
	 * Cache pool config
	 * */
	// JCS Cache Config
	public static String cacheConfig = "/Users/Jelly/Developer/orderQuerySystem/cache.ccf";
	public static int hash_index_block_capacity = 10000;			// 一个索引桶里的数据量

	public static String booleanTrueValue = "true";
	public static String booleanFalseValue = "false";

	public static enum IdName {
		OrderId,BuyerId,GoodId;
	}

	public static enum TableName {
		OrderTable, BuyerTable, GoodTable
	}

	public static enum IndexType {
		OrderId, OrderBuyerId,OrderGoodId
	}
	
	public static enum IdIndexType {
		BuyerIdToOrderId, GoodIdToOrderId
	}
	
	public static String orderId = "orderid";
	public static String buyerId = "buyerid";
	public static String goodId = "goodid";
	public static String createTime = "createtime";
	public static String salerId = "salerid";


	/***
	 * handle file config
	 */
	/*public static int orderFileCapacity = 10000;
	public static int buyerFileCapacity = 10000;
	public static int goodFileCapacity = 10000;
	public static int columnFileCapacity = 10000;*/

	
	
	
	public static String[] storeFolders = null;
	public static String buyerFileNamePrex = "/buyer_";
	public static String goodFileNamePrex = "/good_";
	public static String orderFileNamePrex = "/order_";
	public static String buyerIndexFileSuffix = "_buyerIdIndex";
	public static String goodIndexFileSuffix = "_goodIdIndex";
	public static String orderIndexFileSuffix = "_orderIndex";
	public static String orderBuyerIdIndexFileSuffix = "_orderBuyerIndex";
	public static String orderGoodIdIndexFileSuffix = "_orderGoodIndex";
	public static String buyerSurrFileName = "buyerSurrIndex";
	public static String goodSurrFileName = "goodSurrIndex";
	
	
}
