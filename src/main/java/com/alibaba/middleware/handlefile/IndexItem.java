package com.alibaba.middleware.handlefile;

import com.alibaba.middleware.conf.RaceConfig.IndexType;

public class IndexItem {

	//这里fileName+offset=Value
	String fileName;
	String recordsData = null;
	long offset = 0;
	IndexType indexType = null;

	public IndexItem(String fileName, String recordsData, long offset, IndexType type) {
		this.fileName = fileName;
		this.recordsData = recordsData;
		this.offset = offset;
		this.indexType = type;
	}
	
	public long getOffset() {
		return offset;
	}

	public String getRecords() {
		return recordsData;
	}

	public String getFileName() {
		return fileName;
	}

}
