package com.alibaba.middleware.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Jelly
 *
 * Simple LRU CachePool
 */
public class SimpleCachePool<K, V> implements CachePool<K, V> {
    private final int capacity;
    private LinkedHashMap<K, V> cacheMap;
    private ReadWriteLock lock;

    public SimpleCachePool(final int capacity) {
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
    public void putCache(K key, V value) {
        lock.writeLock().lock();
        try {
            cacheMap.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V getCache(K key) {
        lock.writeLock().lock();
        V value;
        try {
            value = cacheMap.get(key);
        } finally {
            lock.writeLock().unlock();
        }
        return value;
    }
}
