package com.alibaba.middleware.index;

import java.io.ObjectInputStream;
import java.io.RandomAccessFile;

/**
 * 读索引文件的句柄缓冲池
 * @author hankwing
 *
 */
public class BucketReader {

	public RandomAccessFile streamIn = null;
	public ObjectInputStream bucketReader = null;
	
	public BucketReader(RandomAccessFile streanIn, ObjectInputStream bucketReader) {
		this.streamIn = streanIn;
		this.bucketReader = bucketReader;
	}
}
