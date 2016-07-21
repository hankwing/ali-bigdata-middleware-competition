package com.alibaba.middleware.handlefile;

import com.alibaba.middleware.conf.RaceConfig.IndexType;

public class IndexItem {

	//这里dataFileName+offset=Value
	String dataFileName;
	String indexFileName;
	String recordsData = null;
	long offset = 0;
	IndexType indexType = null;

	public IndexItem(String dataFileName, String recordsData, long offset, IndexType type) {
		this.dataFileName = dataFileName;
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

	public String getDataFileName() {
		return dataFileName;
	}
	
	public void setIndexFileName(String indexFileName){
		this.indexFileName = indexFileName;
	}

}
