package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * 订单系统实现
 * 
 * @author hankwing
 *
 */
public class OrderSystemImpl implements OrderSystem {
		
	public static void main(String[] args) {

		// test query
		OrderSystemImpl orderSystem = new OrderSystemImpl();
		
	}
	
	public OrderSystemImpl() {
		// construct
	}

	/**
	 * 初始化  最多1小时
	 */
	public void construct(Collection<String> orderFiles,
			Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException,
			InterruptedException {
			
		
	}

	/**
	   * 查询订单号为orderid的指定字段
	   * 
	   * @param orderid
	   *          订单号
	   * @param keys
	   *          待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
	   * @return 查询结果，如果该订单不存在，返回null
	*/
	public ResultImpl queryOrder(long orderId, Collection<String> keys) {
		// 主要思想：先判断keys在哪个表里  之后根据索引在不同表里找不同字段
		Row query = new Row();
		query.putKV("orderid", orderId);

		return null;
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
	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
			String buyerid) {	
		// 根据买家ID在索引里找到结果 再判断结果是否介于startTime和endTime之间  结果集合按照createTime插入排序
		
		return null;
	}

	/**
	   * 查询某位卖家某件商品所有订单的某些字段
	   * 
	   * @param salerid 卖家Id
	   * @param goodid 商品Id
	   * @param keys 待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
	   * @return 符合条件的订单集合，按照订单id从小至大排序
	*/
	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
			Collection<String> keys) {
		// 根据商品ID找到多条订单信息  再筛选出keys 结果集按照订单id插入排序
		
		return null;
	}

	/**
	   * 对某件商品的某个字段求和，只允许对long和double类型的KV求和 如果字段中既有long又有double，则使用double
	   * 如果求和的key中包含非long/double类型字段，则返回null 如果查询订单中的所有商品均不包含该字段，则返回null
	   * 
	   * @param goodid 商品Id
	   * @param key 求和字段
	   * @return 求和结果
	*/
	public KeyValue sumOrdersByGood(String goodid, String key) {
		// 根据商品ID找到多条订单信息  再根据key值加和
		
		return null;
	}
}
