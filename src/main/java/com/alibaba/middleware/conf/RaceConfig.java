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

	/**
	 * Thread pool config
	 * */
	public static int monitorThreadNum = 1;
    public static int workerThreadNum = Runtime.getRuntime().availableProcessors() * 2;
	public static int queryThreadNum = Runtime.getRuntime().availableProcessors() * 2;
    public static int monitorInitDelayInMills = 1;
    public static int monitorFixedDelayInMills = 5;
    // JVMMonitorThread
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
	
	public static String orderId = "orderid";
	public static String buyerId = "buyerid";
	public static String goodId = "goodid";
	public static String createTime = "createtime";
	public static String salerId = "salerid";
	
}
