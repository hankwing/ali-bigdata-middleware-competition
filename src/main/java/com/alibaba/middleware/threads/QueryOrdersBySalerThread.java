package com.alibaba.middleware.threads;

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
import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.ResultImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.tools.ByteUtils;
import com.alibaba.middleware.tools.BytesKey;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * @author Jelly
 */
public class QueryOrdersBySalerThread extends QueryThread<Iterator<Result>> {
	private String salerid;
	private String goodid;
	private Collection<String> keys;
	private OrderSystemImpl system = null;
	private ConcurrentCache rowCache = null;
	private ByteDirectMemory directMemory = null;

	public QueryOrdersBySalerThread(OrderSystemImpl system, String salerid,
			String goodid, Collection<String> keys) {
		directMemory = ByteDirectMemory.getInstance();
		rowCache = ConcurrentCache.getInstance();
		this.system = system;
		this.salerid = salerid;
		this.goodid = goodid;
		this.keys = keys;
	}

	/**
	 * 判断row是否符合要求 符合则加入结果集
	 * 
	 * @param row
	 * @param results
	 */
	public void handleOffsets(List<byte[]> offsets,
			TreeMap<Long, Result> results, List<String> buyerKeys,
			List<String> goodKeys) {
		try {
			String goodString = null;
			if( !goodKeys.isEmpty()) {
				// 如果要找的key包括了good的key，则不用每次都join 直接将相关字符串加到结果的后面就行
				goodString = system.getRowStringById(TableName.GoodTable, goodid);
			}
			for (byte[] encodedOffset : offsets) {
				// 放入缓冲区
				// 这里要将offset解析成文件下标+offset的形式才能用
				ByteBuffer buffer = ByteBuffer.wrap(encodedOffset);
				int fileIndex = ByteUtils.getIntFromByte(buffer.get());
				long offset = ByteUtils.getLongOffset(buffer.getInt());

				String diskData = RecordsUtils.getStringFromFile(
					system.orderHandlersList.get(fileIndex), offset,
					TableName.OrderTable);
				// 确认一下goodid符合要求
				// rowCache.putInCache(new BytesKey(encodedOffset),
				// diskData, TableName.OrderTable);
				// 放入缓冲区
				StringBuilder resultBuilder = new StringBuilder();
				resultBuilder.append(diskData);
				
				long orderId = Long.valueOf(RecordsUtils.getValueFromLine(diskData, RaceConfig.orderId));
				if (keys == null) {
					resultBuilder.append("\t");
					resultBuilder.append(system.getRowStringById(TableName.BuyerTable, 
							RecordsUtils.getValueFromLine(diskData, RaceConfig.buyerId)));
					resultBuilder.append("\t");
					resultBuilder.append(system.getRowStringById(TableName.GoodTable, goodid));
				} else if (!buyerKeys.isEmpty()) {
					// need query buyerTable
					resultBuilder.append("\t");
					resultBuilder.append(system.getRowStringById(TableName.BuyerTable, 
							RecordsUtils.getValueFromLine(diskData, RaceConfig.buyerId)));
				}
				if (goodString != null) {
					// 需要放入good表里的相关字段即可
					resultBuilder.append("\t").append(goodString);

				}
				results.put(orderId,
						new ResultImpl(orderId, 
								RecordsUtils.createSubKVMapFromLine(resultBuilder.toString(), keys)));

			}
		} catch (TypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 第三个查询 查询某位卖家某件商品所有订单的某些字段
	 * 
	 * @param salerid
	 *            卖家Id
	 * @param goodid
	 *            商品Id
	 * @param keys
	 *            待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
	 * @return 符合条件的订单集合，按照订单id从小至大排序
	 */
	@Override
	public Iterator<Result> call() throws Exception {
		// TODO
		// 根据商品ID找到多条订单信息 再筛选出keys 结果集按照订单id插入排序
		ArrayList<String> buyerKeys = new ArrayList<String>();
		ArrayList<String> goodKeys = new ArrayList<String>();
		if (keys != null) {
			for (String key : keys) {
				if (!key.equals(RaceConfig.buyerId)
						&& system.buyerAttrList.contains(key)) {
					// 如果要找的key不是buyerid并且在buyer表属性里的话 加入buyerKeys
					buyerKeys.add(key);
				} else if (!key.equals(RaceConfig.goodId)
						&& system.goodAttrList.contains(key)) {
					goodKeys.add(key);
				}
			}
		}

		TreeMap<Long, Result> results = new TreeMap<Long, Result>();
		BytesKey surrId = new BytesKey(goodid.getBytes());
		/*boolean isCached = false;

		// 在缓冲区里找对应的order数据
		List<byte[]> orderIds = rowCache.getFromIdCache(surrId,
				IdIndexType.GoodIdToOrderOffsets);
		if( orderIds != null && !orderIds.isEmpty()) {
			// 说明缓冲区里找到了
			isCached = true;
			handleOffsets(orderIds, results, buyerKeys, goodKeys);

		}
		if(!isCached){*/
			// 在索引里找offsetlist
			List<byte[]> offsets = new ArrayList<byte[]>();
			for( int filePathIndex : system.goodIndexMapping.getAllFileIndexs()) {
				DiskHashTable<BytesKey> hashTable = 
						system.goodIdIndexList.get(filePathIndex);
				// 一次性解析所有offset
				byte[] encodedOffsets = hashTable.get(surrId);
				if( encodedOffsets != null) {
					// 找到了
					ByteBuffer tempBuffer = ByteBuffer.wrap(encodedOffsets);
					tempBuffer.position(RaceConfig.byte_size + RaceConfig.compressed_min_bytes_length);
					// 得到所有的byte+offset对
					List<byte[]> byteAndInts = ByteUtils.splitByteBuffer(tempBuffer);
					for( byte[] byteAndOffset : byteAndInts) {
						// 从orderid列表中取出相应的数据
						ByteBuffer buffer = ByteBuffer.wrap(byteAndOffset);
						// 从byte解析出int			
						int fileIndex = ByteUtils.getIntFromByte(buffer.get());
						long offset = buffer.getInt();
						
						// 从文件里读出内容
						offsets.addAll(RecordsUtils.getOrderIdListsFromFile(
								system.goodOrderIdListHandlersList.get(fileIndex), offset));
					}
			
				}
			}

			handleOffsets(offsets, results, buyerKeys, goodKeys);
			// 放入缓冲区
//			rowCache.putInIdCache(surrId, offsets,
//					IdIndexType.GoodIdToOrderOffsets);
		//}


		return results.values().iterator();
	}
}
