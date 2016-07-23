package com.alibaba.middleware.index;

import java.io.FileInputStream;
import java.io.ObjectInputStream;

/**
 * 读索引文件的句柄缓冲池
 * @author hankwing
 *
 */
public class BucketReader {

	public FileInputStream streamIn = null;
	public ObjectInputStream bucketReader = null;
	
	public BucketReader(FileInputStream streanIn, ObjectInputStream bucketReader) {
		this.streamIn = streanIn;
		this.bucketReader = bucketReader;
	}
}
