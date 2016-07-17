package com.alibaba.middleware.cache;

import java.io.Serializable;

/**
 * @author Jelly
 */
public class CacheTest {
    static Cache<Integer, ExampleKV> recordCache = new JCSCache<Integer, ExampleKV>();
    static Cache<Integer, String> simpleCache = new SimpleCache<Integer, String>(10);

    public static void main(String[] args) {
        /**
         * JCSCache test
         * */
        for (int i = 0; i < 5000; i++) {
            ExampleKV kv = new ExampleKV(String.valueOf(i), String.valueOf(i*2));
            recordCache.putInCache(i, kv);
        }
        for (int i = 0; i < 1000; i+=1) {
            ExampleKV kv = recordCache.getFromCache(i);
            if (!kv.getValue().equalsIgnoreCase(String.valueOf(i*2))) {
                System.out.println("Error!");
            }
        }

        /**
         * SimpleCache(LRU) test
         * */
        for (int i = 0; i < 10; i++) {
            simpleCache.putInCache(i, String.valueOf(i));
        }
        simpleCache.putInCache(10, String.valueOf(10));
        for (int i = 0; i < 11; i++) {
            System.out.println("Get value of " + i + ": " + simpleCache.getFromCache(i));
        }
        System.out.println("Get value of " + 2 + ": " + simpleCache.getFromCache(2)); // 2
        System.out.println("Get value of " + 1 + ": " + simpleCache.getFromCache(1)); // 1
        System.out.println("Get value of " + 1 + ": " + simpleCache.getFromCache(1)); // 1
        System.out.println("Get value of " + 2 + ": " + simpleCache.getFromCache(2)); // 2
        System.out.println("Get value of " + 2 + ": " + simpleCache.getFromCache(2)); // 2
        simpleCache.putInCache(11, String.valueOf(11));
        System.out.println("Get value of " + 0 + ": " + simpleCache.getFromCache(0)); // null
        System.out.println("Get value of " + 1 + ": " + simpleCache.getFromCache(1)); // 1
        System.out.println("Get value of " + 3 + ": " + simpleCache.getFromCache(3)); // null
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