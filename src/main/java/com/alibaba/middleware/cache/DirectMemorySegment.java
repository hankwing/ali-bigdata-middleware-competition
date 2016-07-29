package com.alibaba.middleware.cache;

import sun.misc.Unsafe;

/**
 * @author Jelly
 */
public class DirectMemorySegment {

    private long size;

    public DirectMemorySegment(long size) {
        this.size = size;
    }
}
