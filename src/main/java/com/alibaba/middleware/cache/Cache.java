package com.alibaba.middleware.cache;

/**
 * @author Jelly
 */
public interface Cache<K, V> {

    /**
     * Put Cache object into Cache
     * */
    public void putInCache(K key, V value);

    /**
     * Get Cache object from Cache
     * */
    public V getFromCache(K key);
}
