package com.alibaba.middleware.threads;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdIndexType;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.KeyValueImpl;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.ResultImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

/**
 * @author Jelly
 */
public class SumOrdersByGoodThread extends QueryThread<KeyValueImpl> {

	private String goodid;
	private String key;
	private OrderSystemImpl system = null;

	public SumOrdersByGoodThread(OrderSystemImpl system, String goodid,
			String key) {
		this.system = system;
		this.goodid = goodid;
		this.key = key;
	}

	public void addToResults(Row row) {

		// results.put(orderId, new ResultImpl(orderId, row));
	}

	/**
	 * 第四个查询 对某件商品的某个字段求和，只允许对long和double类型的KV求和 如果字段中既有long又有double，则使用double
	 * 如果求和的key中包含非long/double类型字段，则返回null 如果查询订单中的所有商品均不包含该字段，则返回null
	 * 
	 * @param goodid
	 *            商品Id
	 * @param key
	 *            求和字段
	 * @return 求和结果
	 * @throws TypeException
	 */
	@SuppressWarnings("unchecked")
	@Override
	public KeyValueImpl call() throws TypeException {
		// TODO
		// KeyValueImpl result = new KeyValueImpl()

		List<String> keys = new ArrayList<String>();
		boolean isFound = false;
		Long longSum = 0L;
		Double doubleSum = 0.0;
		keys.add(key);

		List<String> buyerKeys = new ArrayList<String>();
		List<String> goodKeys = new ArrayList<String>();
		for (String key : keys) {
			if (system.buyerAttrList.contains(key)) {
				buyerKeys.add(key);
			} else if (system.goodAttrList.contains(key)) {
				goodKeys.add(key);
			}
		}

		Integer surrId = goodid.hashCode();
		if (surrId == 0) {
			return null;
		} else {
			// 先在缓冲区里找
			List<Long> orderIds = system.rowCache.getFormIdCache(surrId,
					IdIndexType.GoodIdToOrderId);
			if (orderIds != null) {
				// 找到了对应的orderid列表
				long orderSize = orderIds.size();
				for (long orderId : orderIds) {
					Row row = system.getRowById(TableName.OrderTable, orderId);
					if (row != null) {
						boolean isGoodKey = false;
						long longValue = 0;
						Double doubleValue = 0.0;
						boolean isLong = true;

						if (!buyerKeys.isEmpty()) {
							// need query buyerTable
							row.putAll(system
									.getRowById(TableName.BuyerTable,
											row.get(RaceConfig.buyerId)
													.valueAsString()));
						}
						if (!goodKeys.isEmpty()) {
							// 到good表里找相应的key
							row.putAll(system.getRowById(TableName.GoodTable,
									row.get(RaceConfig.goodId).valueAsString()));

							try {
								KeyValueImpl keyValue = row.getKV(key);
								if (keyValue != null) {
									isGoodKey = true;
									isFound = true;
									longValue = row.getKV(key).valueAsLong()
											* orderSize;
								} else {
									// 不存在这个key 直接退出
									return null;
								}

							} catch (TypeException e) {
								// TODO Auto-generated catch block
								// 不是long型的
								try {
									isLong = false;
									doubleValue = row.getKV(key)
											.valueAsDouble() * orderSize;
								} catch (TypeException e2) {
									// TODO Auto-generated catch block
									// 不是Double型的 返回Null
									return null;
								}
							}
						}

						KeyValueImpl keyValue = row.getKV(key);
						if (keyValue == null) {
							// // 该条记录不存在这个key
							continue;
						} else if (isGoodKey) {
							// 在good表里找到了这个key
							if (isLong) {
								longSum += longValue;
								break;
							} else {
								doubleSum += doubleValue;
								break;
							}
						} else {
							// 该记录存在该key
							try {
								isFound = true;
								longValue = row.getKV(key).valueAsLong();
							} catch (TypeException e) {
								// TODO Auto-generated catch block
								// 不是long型的
								try {
									isLong = false;
									doubleValue = row.getKV(key)
											.valueAsDouble();
								} catch (TypeException e2) {
									// TODO Auto-generated catch block
									// 不是Double型的 返回Null
									return null;
								}
							}
						}

						if (isLong) {
							longSum += longValue;
						} else {
							doubleSum += doubleValue;
						}
					}
				}

			} else {
				// 在索引里找数据
				for (FilePathWithIndex filePath : system.orderFileList) {
					DiskHashTable<Integer, List<Long>> hashTable = system.orderGoodIdIndexList
							.get(filePath.getFilePath());
					List<Long> offSetresults = hashTable.get(surrId);
					int resultNum = offSetresults.size();
					if (resultNum != 0) {
						for (Long offset : offSetresults) {
							Row row = Row.createKVMapFromLine(RecordsUtils
									.getStringFromFile(filePath, offset,
											TableName.OrderTable));
							boolean isGoodKey = false;
							long longValue = 0;
							Double doubleValue = 0.0;
							boolean isLong = true;

							if (!buyerKeys.isEmpty()) {
								// need query buyerTable
								row.putAll(system.getRowById(
										TableName.BuyerTable,
										row.get(RaceConfig.buyerId)
												.valueAsString()));
							}
							if (!goodKeys.isEmpty()) {
								// 到good表里找相应的key
								row.putAll(system.getRowById(
										TableName.GoodTable,
										row.get(RaceConfig.goodId)
												.valueAsString()));

								try {
									KeyValueImpl keyValue = row.getKV(key);
									if (keyValue != null) {
										isGoodKey = true;
										isFound = true;
										longValue = row.getKV(key)
												.valueAsLong() * resultNum;
									} else {
										// 不存在这个key 直接退出
										return null;
									}

								} catch (TypeException e) {
									// TODO Auto-generated catch block
									// 不是long型的
									try {
										isLong = false;
										doubleValue = row.getKV(key)
												.valueAsDouble() * resultNum;
									} catch (TypeException e2) {
										// TODO Auto-generated catch block
										// 不是Double型的 返回Null
										return null;
									}
								}
							}

							KeyValueImpl keyValue = row.getKV(key);
							if (keyValue == null) {
								// // 该条记录不存在这个key
								continue;
							} else if (isGoodKey) {
								// 在good表里找到了这个key
								if (isLong) {
									longSum += longValue;
									break;
								} else {
									doubleSum += doubleValue;
									break;
								}
							} else {
								// 该记录存在该key
								try {
									isFound = true;
									longValue = row.getKV(key).valueAsLong();
								} catch (TypeException e) {
									// TODO Auto-generated catch block
									// 不是long型的
									try {
										isLong = false;
										doubleValue = row.getKV(key)
												.valueAsDouble();
									} catch (TypeException e2) {
										// TODO Auto-generated catch block
										// 不是Double型的 返回Null
										return null;
									}
								}
							}

							if (isLong) {
								longSum += longValue;
							} else {
								doubleSum += doubleValue;
							}
						}

					}

				}
			}

		}

		if (longSum == 0 && doubleSum == 0 && !isFound) {
			return null;
		} else if (doubleSum == 0) {
			// 返回long 值
			return new KeyValueImpl(key, longSum.toString());
		} else {
			Double doubleReturn = doubleSum + longSum;
			return new KeyValueImpl(key, String.format("%.12f", doubleReturn));
		}
	}
}
