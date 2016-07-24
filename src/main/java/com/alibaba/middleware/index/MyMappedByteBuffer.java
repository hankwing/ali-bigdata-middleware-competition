package com.alibaba.middleware.index;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
/**
 * 利用MappedByteBuffer映射文件
 * @author legend
 *
 */
public class MyMappedByteBuffer{
	public FileChannel channel;
	public MappedByteBuffer mappedBuffer;
	public int mappedStartPostion;
	public int mappedEndPostion;
	public int mappedSize;
	public int lastPostion;

	public MyMappedByteBuffer(FileChannel channel, int mappedSize) {
		// TODO Auto-generated constructor stub
		this.channel = channel;
		this.mappedSize = mappedSize;
		mappedStartPostion = 0;
		lastPostion = mappedStartPostion;
	}

	public void putContent(byte[] content) throws IOException{
		if(mappedBuffer.remaining() > content.length){
			mappedBuffer.put(content);
			mappedBuffer.force();

		}else {
			/**
			 * 写入空闲区域，这块效率有点低啊，没想到好的办法，要复制一份
			 * 内存问题：
			 * 直接内存不属于GC管辖范围，容易造成内存泄漏，自我控制
			 * 写成函数
			 * 
			 */
			int firsetlength = mappedBuffer.remaining();
			int secordlength = content.length - mappedBuffer.remaining();
			byte[] firstbytes= new byte[firsetlength];
			byte[] secordbytes = new byte[secordlength];

			System.arraycopy(content, 0, firstbytes, 0, firsetlength);
			System.arraycopy(content, mappedBuffer.remaining(), secordbytes, 0, secordlength);
			mappedBuffer.put(firstbytes);
			mappedBuffer.force();
			System.gc();//手动gc

			mappedStartPostion = mappedEndPostion;
			mappedEndPostion += mappedSize;
			mappedBuffer = channel.map(MapMode.READ_WRITE, mappedStartPostion, mappedEndPostion);
			mappedBuffer.put(secordbytes);
			mappedBuffer.force();

		}
	}

	public void CoverContent(int positon, byte[] content) throws IOException{
		//确定mappedStartPostion
		if (positon > mappedEndPostion) {
			//从新创建
			while (positon > mappedEndPostion) {
				mappedEndPostion += mappedSize;
			}
			mappedStartPostion = mappedEndPostion - mappedSize;
			mappedBuffer = channel.map(MapMode.READ_WRITE, mappedStartPostion, mappedSize);
		}
		//添加content
		putContent(content);
	}

	public void appendContent(byte[] content) throws IOException{
		if (mappedBuffer == null) {

			mappedBuffer = channel.map(MapMode.READ_WRITE, mappedStartPostion, mappedSize);
			mappedEndPostion = mappedStartPostion + mappedSize;
			mappedBuffer.put(content);
			mappedBuffer.force();

		}
		
		if (lastPostion != mappedStartPostion) {
			mappedBuffer = channel.map(MapMode.READ_WRITE, lastPostion, mappedSize);
		}


		/**
		 * 写入映射文件中，并且强制刷入磁盘
		 */
		if(mappedBuffer.remaining() > content.length){
			mappedBuffer.put(content);
			mappedBuffer.force();

		}else {
			/**
			 * 写入空闲区域，这块效率有点低啊，没想到好的办法，要复制一份
			 * 内存问题：
			 * 直接内存不属于GC管辖范围，容易造成内存泄漏，自我控制
			 * 写成函数
			 * 
			 */
			int firsetlength = mappedBuffer.remaining();
			int secordlength = content.length - mappedBuffer.remaining();
			byte[] firstbytes= new byte[firsetlength];
			byte[] secordbytes = new byte[secordlength];

			System.arraycopy(content, 0, firstbytes, 0, firsetlength);
			System.arraycopy(content, mappedBuffer.remaining(), secordbytes, 0, secordlength);
			mappedBuffer.put(firstbytes);
			mappedBuffer.force();
			System.gc();//手动gc

			mappedStartPostion = mappedEndPostion;
			mappedEndPostion += mappedSize;

			//设置最近一次起始位置
			lastPostion = mappedStartPostion;

			mappedBuffer = channel.map(MapMode.READ_WRITE, mappedStartPostion, mappedEndPostion);
			mappedBuffer.put(secordbytes);
			mappedBuffer.force();
		}
	}

}
