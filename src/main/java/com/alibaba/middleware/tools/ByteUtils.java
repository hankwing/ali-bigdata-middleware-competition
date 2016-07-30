package com.alibaba.middleware.tools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;

public class ByteUtils {  

    public static byte[] longToBytes(long x) {
    	ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(0, x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
    	ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();//need flip 
        return buffer.getLong();
    }

}
