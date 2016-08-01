package com.alibaba.middleware.tools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.Deflater;

import com.alibaba.middleware.conf.RaceConfig;

public class ByteUtils {

	/*
	 * public static byte[] longToBytes(long x) { ByteBuffer buffer =
	 * ByteBuffer.allocate(8); buffer.putLong(0, x); return buffer.array(); }
	 * 
	 * public static long bytesToLong(byte[] bytes) { ByteBuffer buffer =
	 * ByteBuffer.allocate(8); buffer.put(bytes, 0, bytes.length);
	 * buffer.flip();//need flip return buffer.getLong(); }
	 */

	/**
	 * 从byte[]里解析出文件+offset的形式
	 * 
	 * @param line
	 * @return
	 */
	public static List<byte[]> splitBytes(byte[] line) {
		List<byte[]> list = new ArrayList<byte[]>();
		int interval = RaceConfig.compressed_min_bytes_length;
		for (int i = 0; i < line.length; i = i + interval) {
			list.add(Arrays.copyOfRange(line, i, i + interval));
		}
		return list;
	}
	
	public static int byteArrayToLeInt(byte[] encodedValue) {
	    int value = (encodedValue[0] << (Byte.SIZE * 3));
	    value |= (encodedValue[1] & 0xFF) << (Byte.SIZE * 2);
	    value |= (encodedValue[2] & 0xFF) << (Byte.SIZE * 1);
	    value |= (encodedValue[3] & 0xFF);
	    return value;
	}

	public static byte[] leIntToByteArray(int value) {
	    byte[] encodedValue = new byte[Integer.SIZE / Byte.SIZE];
	    encodedValue[0] = (byte) (value >> Byte.SIZE * 3);
	    encodedValue[1] = (byte) (value >> Byte.SIZE * 2);   
	    encodedValue[2] = (byte) (value >> Byte.SIZE);   
	    encodedValue[3] = (byte) value;
	    return encodedValue;
	}

	/**
	 * 解析出所有byte+int对
	 * @param buffer
	 * @return
	 */
	public static List<byte[]> splitByteBuffer(ByteBuffer buffer) {

		List<byte[]> list = new ArrayList<>();
		for (int i = buffer.position(); i < buffer.capacity(); 
				i += RaceConfig.compressed_min_bytes_length) {
			byte[] det = new byte[RaceConfig.compressed_min_bytes_length];
			buffer.get(det);
			list.add(det);
		}
		return list;

	}

	/**
	 * 从byte[]里解析出文件+offset的形式
	 * 
	 * @param line
	 * @return
	 */
	public static byte[] splitBytesAndGetFirst(byte[] line) {
		return Arrays.copyOfRange(line, 0,
				RaceConfig.compressed_min_bytes_length);

	}

	/**
	 * 从byte[]里解析出文件+offset的形式
	 * 
	 * @param line
	 * @return
	 */
	public static byte[] splitBytesAndGetLast(byte[] line) {
		return Arrays.copyOfRange(line, line.length
				- RaceConfig.compressed_min_bytes_length , line.length);

	}

	/**
	 * 将不超过4G的long型offset转换成int
	 * 
	 * @param value
	 * @return
	 */
	public static int getUnsignedInt(long value) {

		return (int) (value - Integer.MAX_VALUE);
	}

	/**
	 * 将int转换成long
	 * 
	 * @param value
	 * @return
	 */
	public static long getLongOffset(int value) {

		return value + 0x7fffffffl;
	}

	public static byte getMagicByteFromInt(int value) {
		return (byte) (value - 128);
	}

	public static int getMagicIntFromByte(byte value) {
		return value + 128;
	}

}
