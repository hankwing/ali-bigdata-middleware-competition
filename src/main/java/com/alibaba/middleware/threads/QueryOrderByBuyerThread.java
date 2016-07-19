package com.alibaba.middleware.threads;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.ResultImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * @author Jelly
 */
public class QueryOrderByBuyerThread extends QueryThread<Iterator<Result>> {
    private long startTime;
    private long endTime;
    private String buyerid;
    private OrderSystemImpl system;

    public QueryOrderByBuyerThread(OrderSystemImpl system, long startTime, long endTime, String buyerid) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.buyerid = buyerid;
        this.system = system;
    }

    /**
     * 第二个查询
	 * 查询某位买家createtime字段从[startTime, endTime) 时间范围内发生的所有订单的所有信息
	 * 
	 * @param startTime
	 *            订单创建时间的下界
	 * @param endTime
	 *            订单创建时间的上界
	 * @param buyerid
	 *            买家Id
	 * @return 符合条件的订单集合，按照createtime大到小排列
	 */
    @SuppressWarnings("unchecked")
	@Override
    public Iterator<Result> call() throws Exception {
    	// 根据买家ID在索引里找到结果 再判断结果是否介于startTime和endTime之间 结果集合按照createTime插入排序
		TreeMap<Long, List<Result>> results = new TreeMap<Long, List<Result>>(
				Collections.reverseOrder());
		long surrId = system.getSurrogateKey(buyerid, IdName.BuyerId);
		if( surrId == 0) {
			//不存在该买家
			return null;
		}
		else {
			for (FilePathWithIndex filePath : system.orderFileList) {
				DiskHashTable<Long, List<Long>> hashTable = system.orderBuyerIdIndexList.get(filePath
						.getFilePath());
				if (hashTable == null) {
					hashTable = system.getHashDiskTable(filePath.getFilePath(),
							filePath.getOrderBuyerIdIndex());
				}
				long resultNum = hashTable.get(surrId).size();
				if (resultNum != 0) {
					// find the records offset
					// 找到后，按照降序插入TreeMap中
					system.orderBuyerIdIndexList.put(filePath.getFilePath(), hashTable);
					System.out.println("records offset:"
							+ resultNum);
					for( Long offset: hashTable.get(surrId)) {
						
						Row row = RecordsUtils.getRecordsByKeysFromFile(
								filePath.getFilePath(), null, offset);
						long createTime = row.getKV(RaceConfig.createTime).valueAsLong();
						if( createTime >= startTime && createTime < endTime) {
							// 判断时间
							List<Result> smallResults = results.get(createTime);
							if(smallResults == null) {
								smallResults = new ArrayList<Result>();
								results.put(createTime, smallResults);
							}
							smallResults.add(new ResultImpl(createTime, row));
						}
						
					}
					
				}

			}
		}
		
		List<Result> returnResults = new ArrayList<Result>();
		for(List<Result> r : results.values() ) {
			returnResults.addAll(r);
		}
		return returnResults.iterator();
    }
}
