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
	/**
	 * channel 文件通道
	 * mappedBuffer 映射缓存
	 * mappedStartPosition 映射文件的起始位置
	 * mappedEndPosition 映射文件的末尾位置
	 * lastPosition 最近的一次映射的文件起始位置
	 */
	public FileChannel channel;
	public MappedByteBuffer mappedBuffer;
	public int mappedStartPosition;
	public int mappedEndPosition;
	public int mappedSize;
	public int lastStartPosition;
	public int lastOffset;

	public MyMappedByteBuffer(FileChannel channel, int mappedSize) {
		// TODO Auto-generated constructor stub
		this.channel = channel;
		this.mappedSize = mappedSize;
		
		mappedStartPosition = 0;
		lastStartPosition = mappedStartPosition;
		lastOffset = 0;
	}

	/**
	 * 当前的映射缓冲区中，在offset的位置中加content
	 * @param content
	 * @throws IOException
	 */
	private void putContent(byte[] content, int offset) throws IOException{
		mappedBuffer.position(offset);
		
		//如果mappedBuffer空闲空间够，则直接添加
		if(mappedBuffer.remaining() > content.length){
			mappedBuffer.put(content);
			lastOffset+=content.length;
		}else {
			
			int firsetlength = mappedBuffer.remaining();
			int secordlength = content.length - mappedBuffer.remaining();
			byte[] firstbytes= new byte[firsetlength];
			byte[] secordbytes = new byte[secordlength];

			System.arraycopy(content, 0, firstbytes, 0, firsetlength);
			System.arraycopy(content, firsetlength, secordbytes, 0, secordlength);
			mappedBuffer.put(firstbytes);
			mappedBuffer.force();
			//手动gc
			System.gc();

			mappedStartPosition = mappedEndPosition;
			mappedBuffer = channel.map(MapMode.READ_WRITE, mappedStartPosition, mappedSize);
			mappedEndPosition += mappedSize;
			mappedBuffer.put(secordbytes);
			mappedBuffer.force();
			lastOffset += secordbytes.length;
			

		}
	}

	/**
	 * 覆盖文件中的position的content,
	 * @param positon
	 * @param content
	 * @throws IOException
	 */
	public void CoverContent(int positon, byte[] content) throws IOException{
		//确定mappedStartPostion
		if (positon > mappedEndPosition) {
			//重新创建映射缓冲区
			while (positon > mappedEndPosition) {
				mappedEndPosition += mappedSize;
			}
			mappedStartPosition = mappedEndPosition - mappedSize;
			mappedBuffer = channel.map(MapMode.READ_WRITE, mappedStartPosition, mappedSize);
		}
		int offset = positon - mappedStartPosition;
		//在现有的mappedBuffer中，添加content
		putContent(content, offset);
	}

	/**
	 * 在映射文件中lastStartPosition中追加content，长度为length
	 * @param content
	 * @param length
	 * @throws IOException
	 */
	public void appendContent(byte[] content) throws IOException{
		//mappedBuffer为空时
		if (mappedBuffer == null) {
			mappedBuffer = channel.map(MapMode.READ_WRITE, mappedStartPosition, mappedSize);
			mappedEndPosition = mappedStartPosition + mappedSize;
			mappedBuffer.put(content);
			mappedBuffer.force();
		}
		
		//mappedBuffer重新映射到末尾
		if (lastStartPosition != mappedStartPosition) {
			mappedBuffer = channel.map(MapMode.READ_WRITE, lastStartPosition, mappedSize);
			mappedBuffer.position(lastOffset);
		}

		if(mappedBuffer.remaining() > content.length){
			mappedBuffer.put(content);
			lastOffset = mappedBuffer.position();
			//写入映射文件中，并且强制刷入磁盘
			mappedBuffer.force();
		}else {
			
			//将content分成两个部分
			int firsetlength = mappedBuffer.remaining();
			int secordlength = content.length - mappedBuffer.remaining();
			byte[] firstbytes= new byte[firsetlength];
			byte[] secordbytes = new byte[secordlength];

			System.arraycopy(content, 0, firstbytes, 0, firsetlength);
			System.arraycopy(content, firsetlength, secordbytes, 0, secordlength);
			mappedBuffer.put(firstbytes);
			mappedBuffer.force();
			//手动gc
			System.gc();

			mappedStartPosition = mappedEndPosition;
			//设置最近一次起始位置
			lastStartPosition = mappedStartPosition;
			mappedBuffer = channel.map(MapMode.READ_WRITE, mappedStartPosition, mappedSize);
			mappedEndPosition += mappedSize;
			mappedBuffer.put(secordbytes);
			mappedBuffer.force();
		}
	}

}
