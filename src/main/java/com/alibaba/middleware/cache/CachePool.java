package com.alibaba.middleware.cache;

/**
 * @author Jelly
 */
public interface CachePool<K, V> {

    /**
     * Put Cache object into CachePool
     * */
    public void putCache(K key, V value);

    /**
     * Get Cache object from CachePool
     * */
    public V getCache(K key);
}
