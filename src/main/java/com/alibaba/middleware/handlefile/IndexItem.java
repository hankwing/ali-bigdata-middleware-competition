package com.alibaba.middleware.handlefile;

import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.race.Row;

public class IndexItem {

	//这里dataFileName+offset=Value
	String dataFileName;
	String indexFileName;
	//String recordsData = null;
	Row rowData = null;
	long offset = 0;

	public IndexItem(String indexFileName, String dataFileName, Row rowData, long offset) {
		this.indexFileName = indexFileName;
		this.dataFileName = dataFileName;
		this.rowData = rowData;
		this.offset = offset;
	}
	
	public long getOffset() {
		return offset;
	}

	public Row getRow() {
		return rowData;
	}

	public String getDataFileName() {
		return dataFileName;
	}
	
	public String getIndexFileName(){
		return indexFileName;
	}

}
