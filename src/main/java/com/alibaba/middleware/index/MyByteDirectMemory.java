package com.alibaba.middleware.index;

import java.nio.ByteBuffer;

public class MyByteDirectMemory {

	public ByteBuffer buffer;
	public int size;
	public int appendPosition;

	/**
	 * 分配一定大小的直接内存字节数组,这个函数调用一次，以后就一直用这个数组
	 * @param size
	 */
	public MyByteDirectMemory(int size) {
		this.size = size;
		buffer = ByteBuffer.allocateDirect(size);
	}

	/**
	 * 获取特定位置，长度的字节数组
	 * @param position
	 * @param length
	 * @return
	 */
	public byte[] getContent(int position,int length){
		byte[] content = new byte[length];
		buffer.position(position);
		buffer.get(content, 0, length);
		return content;
	}

	//覆盖写
	public void coverContent(byte[] content, int position){
		buffer.position(position);
		buffer.put(content);
	}


	//追加在末尾
	public void appendContent(byte[] content){
		buffer.position(appendPosition);

		if (buffer.remaining() > content.length) {
			buffer.put(content);
			appendPosition = buffer.position();
		} else {
			//将content分成两个部分
			int firsetlength = buffer.remaining();
			int secordlength = content.length - buffer.remaining();
			byte[] firstbytes= new byte[firsetlength];
			byte[] secordbytes = new byte[secordlength];

			System.arraycopy(content, 0, firstbytes, 0, firsetlength);
			System.arraycopy(content, firsetlength, secordbytes, 0, secordlength);

			buffer.put(firstbytes);
			//写入文件中

			clearBuffer();
			buffer.put(secordbytes);
			appendPosition = buffer.position();

		}

	}

	//相当于重新开辟一块，从头开始存入数据
	public void clearBuffer(){
		buffer.clear();
		appendPosition = 0;
	}

	public static void main(String args[]){
		//分配1M的内存数组
		MyByteDirectMemory directMemory = new MyByteDirectMemory(1024*1024);

		directMemory.appendContent(int2byte(1));
		directMemory.appendContent(int2byte(3));
		directMemory.appendContent(int2byte(7));

		directMemory.coverContent(int2byte(4), 4);

		System.out.println(byte2int(directMemory.getContent(0, 4)));
		System.out.println(byte2int(directMemory.getContent(4, 4)));
		System.out.println(byte2int(directMemory.getContent(8, 4)));
	}


	public static byte[] int2byte(int res) {  
		byte[] targets = new byte[4];  

		targets[0] = (byte) (res & 0xff);// 最低位   
		targets[1] = (byte) ((res >> 8) & 0xff);// 次低位   
		targets[2] = (byte) ((res >> 16) & 0xff);// 次高位   
		targets[3] = (byte) (res >>> 24);// 最高位,无符号右移。   
		return targets;   
	}   

	public static int byte2int(byte[] res) {    

		int targets = (res[0] & 0xff) | ((res[1] << 8) & 0xff00) // | 表示安位或   
				| ((res[2] << 24) >>> 8) | (res[3] << 24);   
		return targets;   
	} 
}
