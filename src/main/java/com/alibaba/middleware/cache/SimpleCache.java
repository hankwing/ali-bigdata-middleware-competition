package com.alibaba.middleware.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Jelly
 *
 * Simple LRU Cache without interaction with disk
 * Used for caching records
 */
public class SimpleCache<K, V> implements Cache<K, V> {
    private final int capacity;
    private LinkedHashMap<K, V> cacheMap;
    private ReadWriteLock lock;

    public SimpleCache(final int capacity) {
        this.capacity = capacity;
        cacheMap = new LinkedHashMap<K, V>(capacity/2, 0.95f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > capacity;
            }
        };
        lock = new ReentrantReadWriteLock(true);
    }

    @Override
    public void putInCache(K key, V value) {
    	synchronized(cacheMap) {
    		cacheMap.put(key, value);
         }
    }

    @Override
    public V getFromCache(K key) {
    	synchronized(cacheMap) {
            return cacheMap.get(key);
        }
    }
}
