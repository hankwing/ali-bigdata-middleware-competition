package com.alibaba.middleware.cache;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public class SimpleCacheTestThread implements Runnable {
    private SimpleCache<Integer, Integer> cache;
    private String name;

    public SimpleCacheTestThread(SimpleCache<Integer, Integer> cache, String name) {
        this.name = name;
        this.cache = cache;
    }

    @Override
    public void run() {
        for (int i = 0; i < 40; i++) {
            cache.putInCache(i, i);
            System.out.println(name + "->" + cache.getCacheSize());
        }

    }
}
