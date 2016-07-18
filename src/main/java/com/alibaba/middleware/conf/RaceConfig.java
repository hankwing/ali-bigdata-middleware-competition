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
	// 读写线程数
	public static int handleThreadNumber = 1;
	public static int QueueNumber = 10000;					// 一个缓冲队列的大小
	/**
	 * Thread pool config
	 * */
	
	public static int monitorThreadNum = 1;
    public static int workerThreadNum = Runtime.getRuntime().availableProcessors() * 2;
	public static int queryThreadNum = Runtime.getRuntime().availableProcessors() * 2;
	// JVMMonitorThread
    public static int monitorInitDelayInMills = 2;
    public static int monitorFixedDelayInMills = 5;
	public static float memFactor = 0.1f;
	public static int gcCounterThreshold = 2;
	public static int removeBucketNum = 100;

	/**
	 * Cache pool config
	 * */
	// JCS Cache Config
	public static String cacheConfig = "/Users/Jelly/Developer/orderQuerySystem/cache.ccf";
	public static int hash_index_block_capacity = 10000;

	// BucketCachePool
	public static int bucketCachePoolCapacity = 1000;

	public static String booleanTrueValue = "true";
	public static String booleanFalseValue = "false";

	public static enum IdName {
		OrderId,BuyerId,GoodId;
	}

	public static enum TableName {
		OrderTable, BuyerTable, GoodTable
	}

	public static enum IndexType {
		OrderTable, BuyerTable, GoodTable, CountableTable
	}
	
	public static String orderId = "orderid";
	public static String buyerId = "buyerid";
	public static String goodId = "goodid";
	public static String createTime = "createtime";
	public static String salerId = "salerid";


	/***
	 * handle file config
	 */
	public static int orderFileCapacity = 10000;
	public static int buyerFileCapacity = 10000;
	public static int goodFileCapacity = 10000;
	public static int columnFileCapacity = 10000;

	
	public static long smallFileCapacity = 10000000;
	
	public static String[] storeFolders = null;
	public static String buyerFileNamePrex = "/buyer_";
	public static String goodFileNamePrex = "/good_";
	public static String orderFileNamePrex = "/order_";
	public static String buyerIndexFileSuffix = "_buyerIdIndex";
	public static String goodIndexFileSuffix = "_goodIdIndex";
	public static String orderIndexFileSuffix = "_orderIndex";		// orderindex里包含了三种类型的索引
	public static String buyerSurrFileName = "buyerSurrIndex";
	public static String goodSurrFileName = "goodSurrIndex";
	
	
}
