package com.alibaba.middleware.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public class SimpleCacheTest {
    public static void main(String[] args) {
        ExecutorService exe = Executors.newFixedThreadPool(4);
        SimpleCache<Integer, Integer> cache = new SimpleCache<Integer, Integer>(400);
        List<SimpleCacheTestThread> tthreadList = new ArrayList<SimpleCacheTestThread>();

        for (int i = 0; i < 2; i++) {
            SimpleCacheTestThread t = new SimpleCacheTestThread(cache, "T"+i);
            exe.submit(t);
            tthreadList.add(t);
        }

        System.out.println("Main->" + cache.getCacheSize());
        for (int i = 0; i < 10; i++) {
            System.out.println(cache.getFromCache(i));
        }
    }
}
