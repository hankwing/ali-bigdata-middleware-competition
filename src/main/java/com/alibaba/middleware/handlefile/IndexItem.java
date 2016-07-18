package com.alibaba.middleware.handlefile;

public class IndexItem {
	/***
	 * 设置索引的类型
	 */
	int indexType;

	String key;
	//这里fileName+offset=Value
	String fileName;
	long offset;

	public IndexItem(String key, String fileName, long offset, int indexType) {
		this.key = key;
		this.fileName = fileName;
		this.offset = offset;
		this.indexType = indexType;
	}

	public String getKey() {
		return key;
	}

	public String getFileName() {
		return fileName;
	}

	public long getOffset() {
		return offset;
	}

	public int getIndexType() {
		return indexType;
	}

}
