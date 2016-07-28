package com.alibaba.middleware.index;

import java.nio.ByteBuffer;

public class ByteDirectMemory {
	public static ByteBuffer buffer;
	
	public ByteDirectMemory(int size) {
		// TODO Auto-generated constructor stub
		buffer = ByteBuffer.allocateDirect(size);
	}

	public static long getPosition() {
		// TODO Auto-generated method stub
		return buffer.position();
	}

	public static void put(byte[] byteArray) {
		// TODO Auto-generated method stub
		buffer.put(byteArray);
	}

	public static byte[] get(int position, int size) {
		// TODO Auto-generated method stub
		byte[] content = new byte[size];
		buffer.position(position);
		buffer.get(content);
		return content;
	}
	
	public static void clear(){
		buffer.clear();
	}
}
