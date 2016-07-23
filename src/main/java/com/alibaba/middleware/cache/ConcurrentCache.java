package com.alibaba.middleware.cache;

import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.race.Row;

/**
 * @author Jelly
 */
public class ConcurrentCache {
    private ConcurrentLinkedHashMap<Long, String> orderCacheMap;
    private ConcurrentLinkedHashMap<Long, String> buyerCacheMap;
    private ConcurrentLinkedHashMap<Long, String> goodCacheMap;
    private int initCapacity = 1000000;

    /**
     * Init with specified initCapacity
     * */
    public ConcurrentCache(int initCapacity) {
        this.initCapacity = initCapacity;

        orderCacheMap = new ConcurrentLinkedHashMap.Builder<Long, String>()
                .maximumWeightedCapacity(Long.MAX_VALUE)
                .concurrencyLevel(16)
                .initialCapacity(initCapacity)
                .build();
        buyerCacheMap = new ConcurrentLinkedHashMap.Builder<Long, String>()
                .maximumWeightedCapacity(Long.MAX_VALUE)
                .concurrencyLevel(16)
                .initialCapacity(initCapacity)
                .build();
        goodCacheMap = new ConcurrentLinkedHashMap.Builder<Long, String>()
                .maximumWeightedCapacity(Long.MAX_VALUE)
                .concurrencyLevel(16)
                .initialCapacity(initCapacity)
                .build();
    }

    /**
     * Init with default initCapacity as 1000000
     * */
    public ConcurrentCache() {
        orderCacheMap = new ConcurrentLinkedHashMap.Builder<Long, String>()
                .maximumWeightedCapacity(Long.MAX_VALUE)
                .concurrencyLevel(16)
                .initialCapacity(initCapacity)
                .build();
        buyerCacheMap = new ConcurrentLinkedHashMap.Builder<Long, String>()
                .maximumWeightedCapacity(Long.MAX_VALUE)
                .concurrencyLevel(16)
                .initialCapacity(initCapacity)
                .build();
        goodCacheMap = new ConcurrentLinkedHashMap.Builder<Long, String>()
                .maximumWeightedCapacity(Long.MAX_VALUE)
                .concurrencyLevel(16)
                .initialCapacity(initCapacity)
                .build();
    }

    public void putInCache(Long key, String value, TableName tableType) {
        switch (tableType) {
            case OrderTable:
                orderCacheMap.put(key,  value);
                break;
            case BuyerTable:
                buyerCacheMap.put(key, value);
                break;
            case GoodTable:
                goodCacheMap.put(key, value);
                break;
        }
    }

    public Row getFromCache(Long key, TableName tableType) {
        Row row = null;
        switch (tableType) {
            case OrderTable:
                row = Row.createKVMapFromLine(orderCacheMap.get(key));
                break;
            case BuyerTable:
                row = Row.createKVMapFromLine(buyerCacheMap.get(key));
                break;
            case GoodTable:
                row = Row.createKVMapFromLine(goodCacheMap.get(key));
                break;
        }
        return row;
    }

    public void forceEvict(long num) {
        orderEvict(num*2);
        buyerEvict(num);
        goodEvict(num);
    }

    public void orderEvict(long num) {
        orderCacheMap.batchEvict(num);
    }

    public void buyerEvict(long num) {
        buyerCacheMap.batchEvict(num);
    }

    public void goodEvict(long num) {
        goodCacheMap.batchEvict(num);
    }
}
