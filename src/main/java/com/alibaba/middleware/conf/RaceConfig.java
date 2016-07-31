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
	public static long smallFileSizeThreshold = (long) (200* Math.pow(1024, 2));
	
	public static int directMemorySize = (int) (1500* Math.pow(1024, 2));			// 每个索引可使用的directMemory大小
	
	public static int handleThreadNumber = 1;				// 读写线程数
	public static int QueueNumber = 5000;					// 建索引时的一个缓冲队列的大小
	public static int fileHandleNumber = 8;				// 每个文件预先打开的句柄数
	
	//public static int cacheNumberOneRead = 100;					// 每读一次就放入缓冲区中的行的数量
	//public static int bucketNumberOneRead = 50;					// 每读一次桶就放入缓冲区中的桶的数量
	public static long maxIndexFileCapacity = 10000000;				// 一个索引最多存多少行数据
	public static long threIndexFileCapacity = 1000;			// 索引建立xx条后开始向direct memory里写桶
	public static long singleFileMaxLines = 10000000;			// 合并后的文件最大行数
	
	/**
	 * Thread pool config
	 * */
	public static int rowCacheNumber = 5000000;					// 在内存里最多保留几个row数据
	public static int monitorThreadNum = 2;
    public static int workerThreadNum = Runtime.getRuntime().availableProcessors() * 1;
	public static int queryThreadNum = Runtime.getRuntime().availableProcessors() * 1;
	// JVMMonitorThread
    public static int monitorInitDelayInMills = 20 *1000;			// 20s后开始检测内存
    public static int monitorFixedDelayInMills = 5 * 1000;			// 每10s检测一次内存
	public static float memFactor = 0.9f;
	public static float cacheMemFactor = 0.9f;
	public static long forceEvictNum = 1000000;
	public static int gcCounterThreshold = 2;
	public static int removeBucketNum = 100;

	/**
	 * Cache pool config
	 * */
    public static int cacheInitCapacity = 5000;                    // ConcurrentCache中每个队列的初始大小
    public static int cacheMaxCapacity = 100000;                    // ConcurrentCache中每个队列的最大大小
    public static int bucketCapcity = 100;                        // 桶CACHE的最大上限
    public static int bucketRemoveNum = 10;                        // 每次桶的CACHE达到上限后删除一定量的桶

	public static int hash_index_block_capacity = 500;			// 一个索引桶里的数据量
	
	//public static int compressed_max_bytes_lenth = 1024;		// 商品表和买家表索引对应的orderid列表压缩后最大空间
	public static int compressed_min_bytes_length = 5;		// orderid索引对应的orderid列表压缩后最大空间
	public static int compressed_remaining_bytes_length = 2048;


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
		BuyerIdToOrderOffsets, GoodIdToOrderOffsets
	}
	
	public static enum DirectMemoryType {
		BuyerIdSegment, GoodIdSegment, NoWrite
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
	public static String buyerOrderListFileNamePrex = "/buyer_to_orderList_";
	public static String goodOrderListFileNamePrex = "/good_to_orderList_";
	public static String buyerIndexFileSuffix = "_buyerIdIndex";
	public static String goodIndexFileSuffix = "_goodIdIndex";
	public static String orderIndexFileSuffix = "_orderIndex";
	public static String orderBuyerIdIndexFileSuffix = "_orderBuyerIndex";
	public static String orderGoodIdIndexFileSuffix = "_orderGoodIndex";
	public static String buyerSurrFileName = "buyerSurrIndex";
	public static String goodSurrFileName = "goodSurrIndex";
	//public static String indexFileSuffix = "_index";
	
	
	// byte signs
	
	public static byte byte_standfor_direct_memory = 127;				// 代表后面的offset是直接内存的
	public static int byte_size = 1;
	public static byte byte_has_direct_memory_pos = 1;
	public static byte byte_has_no_direct_memory_pos = 0;
	public static int int_size = 4;
	
	
	
}
