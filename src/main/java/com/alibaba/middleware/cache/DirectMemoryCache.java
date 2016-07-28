package com.alibaba.middleware.cache;

import sun.misc.Unsafe;

import java.io.*;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Jelly
 */
public class DirectMemoryCache {

    private Unsafe unsafe;
    private HashMap<Integer, Long> addressMap = new HashMap<Integer, Long>();

    public DirectMemoryCache() {
        try {
            unsafe = getUnsafe();
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

    private int put(int key, List<byte[]> byteList) {
        byte[] bytes = serialize(byteList);
        long baseAddress = unsafe.allocateMemory(bytes.length);
        for (int i = 0; i < bytes.length; i++) {
            unsafe.putByte(baseAddress+i, bytes[i]);
        }
        addressMap.put(key, baseAddress);
        return bytes.length;
    }

    private byte[] serialize(Object obj) {
        try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
            ObjectOutputStream o = new ObjectOutputStream(b);
            o.writeObject(obj);
            return b.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try(ByteArrayInputStream b = new ByteArrayInputStream(bytes)){
            try(ObjectInputStream o = new ObjectInputStream(b)){
                return o.readObject();
            }
        }
    }

    private List<byte[]> get(int key, int size) {
        long baseAddress = addressMap.get(key);
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i ++) {
            bytes[i] = unsafe.getByte(baseAddress+i);
        }
        try {
            return (List<byte[]>) deserialize(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        DirectMemoryCache cache = new DirectMemoryCache();
        List<byte[]> bytes = new ArrayList<>();
        bytes.add(new byte[]{Byte.MAX_VALUE});
        bytes.add(new byte[]{Byte.MIN_VALUE});
        int size = cache.put(1, bytes);
        List<byte[]> byteList = cache.get(1, size);
        for (byte[] byteE: byteList) {
            System.out.println(byteE);
        }
    }
}
