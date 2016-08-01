package com.alibaba.middleware.threads;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.middleware.cache.ConcurrentCache;
import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.conf.RaceConfig.IdIndexType;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.handlefile.FileIndexWithOffset;
import com.alibaba.middleware.index.ByteDirectMemory;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.KeyValueImpl;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.ResultImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.tools.ByteUtils;
import com.alibaba.middleware.tools.BytesKey;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

/**
 * @author Jelly
 */
public class SumOrdersByGoodThread extends QueryThread<KeyValueImpl> {

	private String goodid;
	private String key;
	private OrderSystemImpl system = null;
	private ConcurrentCache rowCache = null;
	private ByteDirectMemory directMemory = null;

	public SumOrdersByGoodThread(OrderSystemImpl system, String goodid,
			String key) {
		directMemory = ByteDirectMemory.getInstance();
		rowCache = ConcurrentCache.getInstance();
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
			if (!key.equals(RaceConfig.buyerId)
					&& system.buyerAttrList.contains(key)) {
				buyerKeys.add(key);
			} else if (!key.equals(RaceConfig.goodId)
					&& system.goodAttrList.contains(key)) {
				goodKeys.add(key);
			}
		}

		BytesKey surrId = new BytesKey(goodid.getBytes());
		boolean isCached = false;

		// 先在缓冲区里找
//		List<byte[]> offsetList = rowCache.getFromIdCache(surrId,
//				IdIndexType.GoodIdToOrderOffsets);
//		if( offsetList != null) {
//			
//			// 说明缓冲区里找到了
//			isCached = true;
//		}
//		if (!isCached) {
			// 说明没有找到 则在索引里找offsets
		List<byte[]> offsetList = new ArrayList<byte[]>();
			for( int filePathIndex : system.goodIndexMapping.getAllFileIndexs()) {
				DiskHashTable<BytesKey> hashTable = 
						system.goodIdIndexList.get(filePathIndex);
				// 一次性解析所有offset
				byte[] encodedOffsets = hashTable.get(surrId);
				if(encodedOffsets != null) {
					// 找到了
					ByteBuffer tempBuffer = ByteBuffer.wrap(encodedOffsets);
					tempBuffer.position(RaceConfig.byte_size + RaceConfig.compressed_min_bytes_length);
					// 得到所有的byte+offset对
					List<byte[]> byteAndInts = ByteUtils.splitByteBuffer(tempBuffer);
					for( byte[] byteAndOffset : byteAndInts) {
						// 从orderid列表中取出相应的数据
						// 从byte解析出int			
						//int fileIndex = ByteUtils.getMagicIntFromByte(buffer.get());
						int memoryIndex = ByteUtils.getMagicIntFromByte(byteAndOffset[0]);
						// 跳过第一个字节
						int pos = ByteUtils.byteArrayToLeInt(Arrays.copyOfRange(byteAndOffset, 
								1, byteAndOffset.length));
						offsetList.addAll(directMemory.getOrderIdListsFromBytes(memoryIndex,pos));
					}
					
				}
			}
			// 放入缓冲区
//			rowCache.putInIdCache(surrId, offsetList,
//					IdIndexType.GoodIdToOrderOffsets);
		//}

		for (byte[] encodedOffset : offsetList) {

			boolean isGoodKey = false;
			boolean isLong = true;
			long longValue = 0;
			double doubleValue = 0;

			// Row row = rowCache.getFromCache(new BytesKey(encodedOffset),
			// TableName.OrderTable);
			ByteBuffer buffer = ByteBuffer.wrap(encodedOffset);
			int fileIndex = ByteUtils.getMagicIntFromByte(buffer.get());
			long offset = ByteUtils.getLongOffset(buffer.getInt());

			// if(row != null) {
			// row =
			// row.getKV(RaceConfig.goodId).valueAsString().equals(goodid) ?
			// row : RecordsUtils.createKVMapFromLine(
			// RecordsUtils.getStringFromFile(system.orderHandlersList.get(fileIndex),
			// offset, TableName.OrderTable));
			// }
			// else {
			String diskData = RecordsUtils.getStringFromFile(
					system.orderHandlersList.get(fileIndex), offset,
					TableName.OrderTable);
//			if (RecordsUtils.getValueFromLine(diskData, RaceConfig.goodId)
//					.equals(goodid)) {
				// 确认goodid相同
				StringBuilder resultBuilder = new StringBuilder();
				resultBuilder.append(diskData);
				if (!buyerKeys.isEmpty()) {
					// need query buyerTable
					resultBuilder.append("\t");
					resultBuilder.append(system.getRowStringById(TableName.BuyerTable, 
							RecordsUtils.getValueFromLine(diskData, RaceConfig.buyerId)));
				}
				if (!goodKeys.isEmpty()) {
					// 到good表里找相应的key	
					resultBuilder.append("\t");
					resultBuilder.append(system.getRowStringById(TableName.GoodTable, 
							RecordsUtils.getValueFromLine(diskData, RaceConfig.goodId)));
					Row row = RecordsUtils.createKVMapFromLine(resultBuilder.toString());
					try {
						KeyValueImpl keyValue = row.getKV(key);
						if (keyValue != null) {
							isGoodKey = true;
							isFound = true;
							longSum = row.getKV(key).valueAsLong()
									* offsetList.size();
							break;
						} else {
							// 不存在这个key 直接退出
							return null;
						}

					} catch (TypeException e) {
						// TODO Auto-generated catch block
						// 不是long型的
						try {
							isLong = false;
							doubleSum = row.getKV(key).valueAsDouble()
									* offsetList.size();
							break;
						} catch (TypeException e2) {
							// TODO Auto-generated catch block
							// 不是Double型的 返回Null
							return null;
						}
					}
				}
				
				Row row = RecordsUtils.createKVMapFromLine(resultBuilder.toString());
				KeyValueImpl keyValue = row.getKV(key);
				if (keyValue == null) {
					// // 该条记录不存在这个key
					continue;
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
							doubleValue = row.getKV(key).valueAsDouble();
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

		//}

		if (longSum == 0 && doubleSum == 0 && !isFound) {
			return null;
		} else if (doubleSum == 0) {
			// 返回long 值
			return new KeyValueImpl(key, longSum.toString());
		} else {
			Double doubleReturn = doubleSum + longSum;
			return new KeyValueImpl(key, String.format("%.16f", doubleReturn));
		}
	}
	
	/**
	 * 从offsetList中得到符合要求的offset的个数  避免hashcode重复
	 * @param offsetList
	 * @return
	 */
	/*public int getCorrectSize( List<byte[]> offsetList) {
		int count = 0;
		for (byte[] encodedOffset : offsetList) {
			ByteBuffer buffer = ByteBuffer.wrap(encodedOffset);
			int fileIndex = buffer.getInt();
			long offset = buffer.getLong();

			String diskData = RecordsUtils.getStringFromFile(
					system.orderHandlersList.get(fileIndex), offset,
					TableName.OrderTable);
			if (RecordsUtils.getValueFromLine(diskData, RaceConfig.goodId)
					.equals(goodid)) {
				count ++;
			}
		}
		return count;
	}*/
}
