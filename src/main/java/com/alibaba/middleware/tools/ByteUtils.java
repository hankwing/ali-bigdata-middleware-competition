package com.alibaba.middleware.tools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.Deflater;

import com.alibaba.middleware.conf.RaceConfig;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFException;

public class ByteUtils {  

    /*public static byte[] longToBytes(long x) {
    	ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(0, x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
    	ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();//need flip 
        return buffer.getLong();
    }*/
    
    /**
     * 从byte[]里解析出文件+offset的形式
     * @param line
     * @return
     */
    public static List<byte[]> splitBytes(byte[] line){
		List<byte[]> list = new ArrayList<byte[]>();
		int interval = RaceConfig.compressed_min_bytes_length;
		for (int i = 0; i < line.length; i= i+interval) {
			list.add(Arrays.copyOfRange(line, i, i+interval));
		}
		return list;
	}
    
    /**
     * 从byte[]里解析出文件+offset的形式
     * @param line
     * @return
     */
    public static byte[] splitBytesAndGetFirst(byte[] line){
    	
		try {
			return Arrays.copyOfRange(LZFDecoder.decode(line), 0, 
					RaceConfig.compressed_min_bytes_length);
		} catch (LZFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}

}
