package com.alibaba.middleware.cache;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.BytesKey;
import com.alibaba.middleware.tools.RecordsUtils;

import java.util.List;

/**
 * @author Jelly
 */
public class ConcurrentCache {
	// 先不保存order表里的字段了  因为命中率较低
    //private ConcurrentLinkedHashMap<BytesKey, String> orderCacheMap;
    private ConcurrentLinkedHashMap<Integer, String> buyerCacheMap;
    private ConcurrentLinkedHashMap<Integer, String> goodCacheMap;

    private ConcurrentLinkedHashMap<Integer, List<byte[]>> buyerToOrderIdCacheMap;
    private ConcurrentLinkedHashMap<Integer, List<byte[]>> goodToOrderIdCacheMap;

    private int initCapacity = RaceConfig.cacheInitCapacity;
    private int maxCapacity = RaceConfig.cacheMaxCapacity;

    private static ConcurrentCache instance = null;

    /**
     * Init with default initCapacity as 1000000
     * */
    private ConcurrentCache() {
    	initCapacity = RaceConfig.cacheInitCapacity;
    	
        /*orderCacheMap = new ConcurrentLinkedHashMap.Builder<BytesKey, String>()
                .maximumWeightedCapacity(Long.MAX_VALUE)
>>>>>>> 43bbbe142a4f78555c41b992fedfbd6f0894ba92
                .concurrencyLevel(16)
                .initialCapacity(initCapacity)
                .build();*/
        buyerCacheMap = new ConcurrentLinkedHashMap.Builder<Integer, String>()
                .maximumWeightedCapacity(maxCapacity)
                .concurrencyLevel(16)
                .initialCapacity(initCapacity)
                .build();
        goodCacheMap = new ConcurrentLinkedHashMap.Builder<Integer, String>()
                .maximumWeightedCapacity(maxCapacity)
                .concurrencyLevel(16)
                .initialCapacity(initCapacity)
                .build();

        buyerToOrderIdCacheMap = new ConcurrentLinkedHashMap.Builder<Integer, List<byte[]>>()
                .maximumWeightedCapacity(maxCapacity)
                .concurrencyLevel(16)
                .initialCapacity(initCapacity)
                .build();
        goodToOrderIdCacheMap = new ConcurrentLinkedHashMap.Builder<Integer, List<byte[]>>()
                .maximumWeightedCapacity(maxCapacity)
                .concurrencyLevel(16)
                .initialCapacity(initCapacity)
                .build();
    }

    /**
     * Get the ConcurrentCache instance
     * */
    public static ConcurrentCache getInstance() {
        if (instance == null) {
            instance = new ConcurrentCache();
        }
        return instance;
    }

    public void putInCache(Object key, String value, TableName tableType) {
        switch (tableType) {
            case OrderTable:
                //orderCacheMap.put((BytesKey) key,  value);
                break;
            case BuyerTable:
                buyerCacheMap.put((Integer) key, value);
                break;
            case GoodTable:
                goodCacheMap.put((Integer) key, value);
                break;
        }
    }

    public void putInIdCache(Integer key, List<byte[]> value, RaceConfig.IdIndexType indexType) {
        switch (indexType) {
            case BuyerIdToOrderOffsets:
                if (!buyerToOrderIdCacheMap.containsKey(key))
                    buyerToOrderIdCacheMap.put(key, value);
                break;
            case GoodIdToOrderOffsets:
                if (!goodToOrderIdCacheMap.containsKey(key))
                    goodToOrderIdCacheMap.put(key, value);
                break;
        }
    }

    /*public Row getFromCache(Object key, TableName tableType) {
        Row row = null;
        switch (tableType) {
            case OrderTable:
                //row = RecordsUtils.createKVMapFromLine(orderCacheMap.get(key));
                break;
            case BuyerTable:
                row = RecordsUtils.createKVMapFromLine(buyerCacheMap.get(key));
                break;
            case GoodTable:
                row = RecordsUtils.createKVMapFromLine(goodCacheMap.get(key));
                break;
        }
        return row;
    }*/

    public List<byte[]> getFromIdCache(Integer key, RaceConfig.IdIndexType indexType) {
        List<byte[]> cache = null;
        switch (indexType) {
            case BuyerIdToOrderOffsets:
                cache = buyerToOrderIdCacheMap.get(key);
                break;
            case GoodIdToOrderOffsets:
                cache = goodToOrderIdCacheMap.get(key);
                break;
        }
        return cache;
    }

    public void forceEvict(long num) {
        //long orderS = orderCacheMap.size();
        long buyerS = buyerCacheMap.size();
        long goodS = goodCacheMap.size();
        long goodIdS = goodToOrderIdCacheMap.size();
        long buyerIdS = buyerToOrderIdCacheMap.size();
        long size = buyerS + goodS + goodIdS + buyerIdS;
        //orderEvict(num*orderS/size);
        buyerEvict(num*buyerS/size);
        goodEvict(num*goodS/size);
        goodToOrderIdEvict(num*goodIdS/size);
        buyerToOrderIdEvict(num*buyerIdS/size);
    }

    //public void orderEvict(long num) {
    //    orderCacheMap.batchEvict(num);
    //}

    public void buyerEvict(long num) {
        buyerCacheMap.batchEvict(num);
    }

    public void goodEvict(long num) {
        goodCacheMap.batchEvict(num);
    }

    public void goodToOrderIdEvict(long num) {
        goodToOrderIdCacheMap.batchEvict(num);
    }

    public void buyerToOrderIdEvict(long num) {
        buyerToOrderIdCacheMap.batchEvict(num);
    }

    /**
     * No use
     * just for test
     * */

    public int getSize() {
        return buyerCacheMap.size();
    }
}
