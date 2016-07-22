package com.alibaba.middleware.handlefile;

import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.race.Row;

public class IndexItem {

	//这里dataFileName+offset=Value
	String dataFileName;
	String indexFileName;
	String recordsData = null;
	//Row rowData = null;
	long offset = 0;

	public IndexItem(String indexFileName, String dataFileName, String recordsData, long offset) {
		this.indexFileName = indexFileName;
		this.dataFileName = dataFileName;
		this.recordsData = recordsData;
		this.offset = offset;
	}
	
	public long getOffset() {
		return offset;
	}

	public String getRecordsData() {
		return recordsData;
	}

	public String getDataFileName() {
		return dataFileName;
	}
	
	public String getIndexFileName(){
		return indexFileName;
	}

}
