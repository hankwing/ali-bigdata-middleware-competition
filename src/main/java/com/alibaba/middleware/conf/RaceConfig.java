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
    public static int workerThreadNum = Runtime.getRuntime().availableProcessors();
    public static int monitorInitDelayInMills = 1;
    public static int monitorFixedDelayInMills = 5;

	/**
	 * Cache pool config
	 * */
	public static String cacheConfig = "/Users/Jelly/Developer/orderQuerySystem/cache.ccf";
	public static int hash_index_block_capacity = 10000;
	
	public static String booleanTrueValue = "true";
	public static String booleanFalseValue = "false";
	
}
