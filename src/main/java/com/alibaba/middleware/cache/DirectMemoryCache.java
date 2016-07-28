package com.alibaba.middleware.cache;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;

/**
 * @author Jelly
 */
public class DirectMemoryCache {

    private Unsafe unsafe;
    private HashMap<Integer, Long> addressMap = new HashMap<Integer, Long>();
    private long memAddress;

    public DirectMemoryCache(long size) {
        try {
            unsafe = getUnsafe();
            memAddress = unsafe.allocateMemory(size);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private Unsafe getUnsafe() throws NoSuchFieldException, IllegalAccessException {
        Field field = Unsafe.class.getDeclaredField("theUnsafe");
        field.setAccessible(true);
        return (Unsafe) field.get(null);
    }

    private boolean put(int key, List<byte[]> byteList) {

    }
}
