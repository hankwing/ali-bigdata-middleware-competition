package com.alibaba.middleware.cache;

import java.io.Serializable;

/**
 * @author Jelly
 */
public class CachePoolTest {
    static CachePool<Integer, ExampleKV> recordCache = new JCSCachePool<Integer, ExampleKV>();
    static CachePool<Integer, String> simpleCache = new SimpleCachePool<Integer, String>(10);

    public static void main(String[] args) {
        /**
         * JCSCachePool test
         * */
        for (int i = 0; i < 5000; i++) {
            ExampleKV kv = new ExampleKV(String.valueOf(i), String.valueOf(i*2));
            recordCache.putCache(i, kv);
        }
        for (int i = 0; i < 1000; i+=1) {
            ExampleKV kv = recordCache.getCache(i);
            if (!kv.getValue().equalsIgnoreCase(String.valueOf(i*2))) {
                System.out.println("Error!");
            }
        }

        /**
         * SimpleCachePool(LRU) test
         * */
        for (int i = 0; i < 10; i++) {
            simpleCache.putCache(i, String.valueOf(i));
        }
        simpleCache.putCache(10, String.valueOf(10));
        for (int i = 0; i < 11; i++) {
            System.out.println("Get value of " + i + ": " + simpleCache.getCache(i));
        }
        System.out.println("Get value of " + 2 + ": " + simpleCache.getCache(2)); // 2
        System.out.println("Get value of " + 1 + ": " + simpleCache.getCache(1)); // 1
        System.out.println("Get value of " + 1 + ": " + simpleCache.getCache(1)); // 1
        System.out.println("Get value of " + 2 + ": " + simpleCache.getCache(2)); // 2
        System.out.println("Get value of " + 2 + ": " + simpleCache.getCache(2)); // 2
        simpleCache.putCache(11, String.valueOf(11));
        System.out.println("Get value of " + 0 + ": " + simpleCache.getCache(0)); // null
        System.out.println("Get value of " + 1 + ": " + simpleCache.getCache(1)); // 1
        System.out.println("Get value of " + 3 + ": " + simpleCache.getCache(3)); // null
    }
}

class ExampleKV implements Serializable {
    private static final long serialVersionUID = 6392376146163510146L;

    private String key;
    private String value;

    public ExampleKV(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}