package com.alibaba.middleware.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alibaba.middleware.conf.RaceConfig;
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
    private LinkedHashMap<Long, String> orderCacheMap;
    private LinkedHashMap<Long,String> buyerCacheMap;
    private LinkedHashMap<Long, String> goodCacheMap;
    //private LinkedHashMap<Integer, List<Row>> orderBuyerIdCacheMap;
    //private LinkedHashMap<Integer, List<Row>> orderGoodIdCacheMap;
    //private LinkedHashMap<Integer, Row> buyerCacheMap;
    //private LinkedHashMap<Integer, Row> goodCacheMap;
    private ReadWriteLock lock;
    private static SimpleCache instance = null;
    
    public static SimpleCache getInstance() {
        if (instance == null)
            instance = new SimpleCache( RaceConfig.rowCacheNumber);
        return instance;
    }

    public SimpleCache(final int capacity) {
        this.capacity = capacity;
        orderCacheMap = new LinkedHashMap<Long, String>(capacity/2, 0.95f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, String> eldest) {
                return size() > capacity;
            }
        };
        
        buyerCacheMap = new LinkedHashMap<Long, String>(capacity/2, 0.95f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, String> eldest) {
                return size() > capacity;
            }
        };
        
        goodCacheMap = new LinkedHashMap<Long, String>(capacity/2, 0.95f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, String> eldest) {
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
    
    public void putInCache(Long key, String value, TableName tableType) {
    	switch( tableType) {
    	case OrderTable:
    		//synchronized(orderCacheMap) {
    		lock.writeLock().lock();
    		orderCacheMap.put( key, value);
    		lock.readLock().unlock();
            // }
    		break;
    	case BuyerTable:
    		//synchronized(buyerCacheMap) {
    		lock.writeLock().lock();
    		buyerCacheMap.put( key, value);
    		lock.writeLock().unlock();
            // }
    		break;
    	case GoodTable:
    		//synchronized(goodCacheMap) {
    		lock.writeLock().lock();
    		goodCacheMap.put( key, value);
    		lock.writeLock().unlock();
            // }
    		break;
    	}
    	
    }
    
    /**
     * 插入order表里的以buyerid或goodid为key的row缓存数据
     * @param key
     * @param orderId
     * @param value
     * @param tableType
     */
   /* public void putInListCache(Integer key,Long orderId, Row value, TableName tableType) {
    	switch( tableType) {
    	case OrderTable:
    		break;
    	case BuyerTable:
    		synchronized(orderBuyerIdCacheMap ) {
    			List<Row> result = orderBuyerIdCacheMap.get(key);
    			Row temp = null;
    			synchronized(orderCacheMap ) {
    				// 插入一条数据到orderBuyer的时候，先看数据在orderCache里出现了没有
    				temp = orderCacheMap.get(orderId);
    			}
    			
    			if( temp != null) {
    				// 在orderCache里找到了
    				value = temp;
    			}
    			if( result == null) {
    				result = new ArrayList<Row>();
    				orderBuyerIdCacheMap.put(key, result);
    			}
    			result.add(value);
             }
    		break;
    	case GoodTable:
    		synchronized(orderGoodIdCacheMap ) {
    			List<Row> result = orderGoodIdCacheMap.get(key);
    			Row temp = null;
    			synchronized(orderCacheMap ) {
    				// 插入一条数据到orderBuyer的时候，先看数据在orderCache里出现了没有
    				temp = orderCacheMap.get(orderId);
    			}
    			
    			if( temp != null) {
    				// 在orderCache里找到了
    				value = temp;
    			}
    			if( result == null) {
    				result = new ArrayList<Row>();
    				orderGoodIdCacheMap.put(key, result);
    			}
    			result.add(value);
             }
    		break;
    	}
    	
    }*/

    public Row getFromCache(long key, TableName tableType) {
    	Row row = null;
    	switch( tableType) {
    	case OrderTable:
    		//synchronized(orderCacheMap) {
    		lock.readLock().lock();
    		row = Row.createKVMapFromLine(orderCacheMap.get(key));
    		lock.readLock().unlock();
            // }
    	case BuyerTable:
    		//synchronized(buyerCacheMap) {
    		lock.readLock().lock();
    		row = Row.createKVMapFromLine(buyerCacheMap.get(key));
    		lock.readLock().unlock();
            // }
    	case GoodTable:
    		//synchronized(goodCacheMap) {
    		lock.readLock().lock();
    		row = Row.createKVMapFromLine(goodCacheMap.get(key));
    		lock.readLock().unlock();
            // }
    	}
    	return row;
    }

	/*public void putInCache(int key, Row row,
			TableName tableType) {
		// TODO Auto-generated method stub
		switch( tableType) {
    	case OrderTable:
    		synchronized(orderCacheMap) {
    			
    			orderCacheMap.put((long) key, row);
             }
    		break;
    	case BuyerTable:
    		synchronized(buyerCacheMap) {
    			buyerCacheMap.put( (long) key, row);
             }
    		break;
    	case GoodTable:
    		synchronized(goodCacheMap) {
    			goodCacheMap.put( (long) key, row);
             }
    		break;
    	}
		
	}*/
    
    /*public List<Row> getRowListFromCache(Integer key, TableName tableType) {
    	List<Row> results = null;
    	switch( tableType) {
    	case BuyerTable:
    		synchronized(buyerCacheMap) {
    			results =  orderBuyerIdCacheMap.get(key);
             }
    	case GoodTable:
    		synchronized(goodCacheMap) {
    			results =  orderGoodIdCacheMap.get(key);
             }
		case OrderTable:
			break;
		default:
			break;
    	}
    	return results;
    }*/

    // unsafe, just for test
    /*public int getCacheSize() {
        return cacheMap.size();
    }*/
}
