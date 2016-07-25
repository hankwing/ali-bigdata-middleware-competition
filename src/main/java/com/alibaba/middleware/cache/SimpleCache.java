package com.alibaba.middleware.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdIndexType;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.RecordsUtils;

/**
 * @author Jelly
 *
 * Simple LRU Cache without interaction with disk
 * Used for caching records
 */
public class SimpleCache {
    private final int capacity;
    
    private LinkedHashMap<byte[], String> orderCacheMap;					// 存的是hashcode+offset-->string
    private LinkedHashMap<Integer, String> buyerCacheMap;				// 存的是id-->string
    private LinkedHashMap<Integer, String> goodCacheMap;				// 存的是id-->string
    private LinkedHashMap<Integer, List<byte[]>> buyerToOrderIdCacheMap;	// 存的是key-->hashcode+offset
    private LinkedHashMap<Integer, List<byte[]>> goodToOrderIdCacheMap;	// 存的是key-->hashCode+offset
    //private LinkedHashMap<Integer, Row> buyerCacheMap;
    //private LinkedHashMap<Integer, Row> goodCacheMap;
    private ReadWriteLock orderLock;
    private ReadWriteLock buyerLock;
    private ReadWriteLock goodLock;
    private ReadWriteLock buyerToOrderLock;
    private ReadWriteLock goodToOrderLock;
    private static SimpleCache instance = null;

    /*public static SimpleCache getInstance() {
        if (instance == null)
            instance = new SimpleCache( RaceConfig.rowCacheNumber);
        return instance;
    }*/

    public SimpleCache(final int capacity) {
        this.capacity = capacity;
        orderCacheMap = new LinkedHashMap<byte[], String>(capacity/2, 0.95f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<byte[], String> eldest) {
                return size() > capacity;
            }
        };
        
        buyerCacheMap = new LinkedHashMap<Integer, String>(capacity/2, 0.95f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest) {
                return size() > capacity;
            }
        };
        
        goodCacheMap = new LinkedHashMap<Integer, String>(capacity/2, 0.95f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest) {
                return size() > capacity;
            }
        };
        
        buyerToOrderIdCacheMap = new LinkedHashMap<Integer, List<byte[]>>(capacity/2, 0.95f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, List<byte[]>> eldest) {
                return size() > capacity;
            }
        };
        
        goodToOrderIdCacheMap = new LinkedHashMap<Integer, List<byte[]>>(capacity/2, 0.95f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, List<byte[]>> eldest) {
                return size() > capacity;
            }
        };
        
        orderLock = new ReentrantReadWriteLock(false);
        buyerLock = new ReentrantReadWriteLock(false);
        goodLock = new ReentrantReadWriteLock(false);
        buyerToOrderLock = new ReentrantReadWriteLock(false);
        goodToOrderLock = new ReentrantReadWriteLock(false);
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
    
    public void putInCache(Object key, String value, TableName tableType) {
    	switch( tableType) {
    	case OrderTable:
    		//synchronized(orderCacheMap) {
    		orderLock.writeLock().lock();
    		orderCacheMap.put( (byte[]) key, value);
    		orderLock.writeLock().unlock();
            // }
    		break;
    	case BuyerTable:
    		//synchronized(buyerCacheMap) {
    		buyerLock.writeLock().lock();
    		buyerCacheMap.put( (Integer) key, value);
    		buyerLock.writeLock().unlock();
            // }
    		break;
    	case GoodTable:
    		//synchronized(goodCacheMap) {
    		goodLock.writeLock().lock();
    		goodCacheMap.put( (Integer) key, value);
    		goodLock.writeLock().unlock();
            // }
    		break;
    	}
    	
    }
    
    /**
     * 缓存order表里的buyerid和goodid对应的order表里的offset列表
     * @param key
     * @param value
     * @param tableType
     */
    public void putInIdCache(Integer key, List<byte[]> offsets, IdIndexType indexType) {
    	switch( indexType) {
    	case BuyerIdToOrderOffsets:
    		//synchronized(orderCacheMap) {
    		buyerToOrderLock.writeLock().lock();
    		List<byte[]> list = buyerToOrderIdCacheMap.get(key);
    		if( list == null) {
    			buyerToOrderIdCacheMap.put(key, offsets);
    		}
    		buyerToOrderLock.writeLock().unlock();
    		break;
    	case GoodIdToOrderOffsets:
    		goodToOrderLock.writeLock().lock();
    		List<byte[]> goodList = goodToOrderIdCacheMap.get(key);
    		if( goodList == null) {
    			goodToOrderIdCacheMap.put(key, offsets);
    		}
    		goodToOrderLock.writeLock().unlock();
    		break;
    	}
    }

    /**
     * 根据buyerid或者goodid得到对应的orderid的offset列表
     * @param key
     * @param indexType
     * @return
     */
    public List<byte[]> getFormIdCache(Integer key, IdIndexType indexType) {
    	List<byte[]> results = null;
    	switch( indexType) {
    	case BuyerIdToOrderOffsets:
    		//synchronized(orderCacheMap) {
    		buyerToOrderLock.readLock().lock();
    		results = buyerToOrderIdCacheMap.get(key);
    		buyerToOrderLock.readLock().unlock();
    		break;
    	case GoodIdToOrderOffsets:
    		goodToOrderLock.readLock().lock();
    		results = goodToOrderIdCacheMap.get(key);
    		goodToOrderLock.readLock().unlock();
    		break;
    	}
    	return results;
    	
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

    public Row getFromCache(Object key, TableName tableType) {
    	Row row = null;
    	switch( tableType) {
    	case OrderTable:
    		//synchronized(orderCacheMap) {
    		orderLock.readLock().lock();
    		row = RecordsUtils.createKVMapFromLine(orderCacheMap.get(key));
    		orderLock.readLock().unlock();
    		break;
            // }
    	case BuyerTable:
    		//synchronized(buyerCacheMap) {
    		buyerLock.readLock().lock();
    		row = RecordsUtils.createKVMapFromLine(buyerCacheMap.get(key));
    		buyerLock.readLock().unlock();
    		break;
            // }
    	case GoodTable:
    		//synchronized(goodCacheMap) {
    		goodLock.readLock().lock();
    		row = RecordsUtils.createKVMapFromLine(goodCacheMap.get(key));
    		goodLock.readLock().unlock();
    		break;
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
