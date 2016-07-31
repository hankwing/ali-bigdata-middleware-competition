package com.alibaba.middleware.handlefile;

/**
 * 从byte数组里解码出这个类 可以获得文件下标index和文件内的offset
 * @author hankwing
 *
 */
public class FileIndexWithOffset {

	public byte fileIndex = 0;
	public long offset = 0;
	
	public FileIndexWithOffset( byte fileIndex, long offset) {
		this.fileIndex = fileIndex;
		this.offset = offset;
	}
}
