package com.alibaba.middleware.cache;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.race.Row;

/**
 * @author Jelly
 *
 * Simple LRU Cache without interaction with disk
 * Used for caching records
 */
public class SimpleCache {
    private final int capacity;
    private LinkedHashMap<Long, List<Row>> orderCacheMap;
    private LinkedHashMap<Integer, List<Row>> buyerCacheMap;
    private LinkedHashMap<Integer, List<Row>> goodCacheMap;
    private ReadWriteLock lock;

    public SimpleCache(final int capacity) {
        this.capacity = capacity;
        orderCacheMap = new LinkedHashMap<Long, List<Row>>(capacity/2, 0.95f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, List<Row>> eldest) {
                return size() > capacity;
            }
        };
        
        buyerCacheMap = new LinkedHashMap<Integer, List<Row>>(capacity/2, 0.95f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, List<Row>> eldest) {
                return size() > capacity;
            }
        };
        
        goodCacheMap = new LinkedHashMap<Integer, List<Row>>(capacity/2, 0.95f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, List<Row>> eldest) {
                return size() > capacity;
            }
        };
        
        lock = new ReentrantReadWriteLock(false);
    }

    /*@Override
    public void putInCache(K key, V value) {
    	synchronized(cacheMap) {
    		cacheMap.put(key, value);
         }
    }

    @Override
    public V getFromCache(K key) {

        lock.readLock().lock();
        V value;
        try {
            value = cacheMap.get(key);
        } finally {
            lock.readLock().unlock();
        }
        return value;
    }*/
    
    /*public void putInCache(long key, Row value, TableName tableType) {
    	switch( tableType) {
    	case OrderTable:
    		synchronized(orderCacheMap) {
    			
    			orderCacheMap.put(key, value);
             }
    		break;
    	case BuyerTable:
    		synchronized(orderCacheMap) {
    			orderCacheMap.put(key, value);
             }
    		break;
    	case GoodTable:
    		synchronized(orderCacheMap) {
    			orderCacheMap.put(key, value);
             }
    		break;
    	}
    	
    }

    public V getFromCache(K key) {
    	synchronized(cacheMap) {
            return cacheMap.get(key);
        }
    }

    // unsafe, just for test
    public int getCacheSize() {
        return cacheMap.size();
    }*/
}
