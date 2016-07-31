package com.alibaba.middleware.handlefile;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.tools.RecordsUtils;

public class IndexItem {

	//这里dataFileName+offset=Value
	int dataSerialNumber;
	String indexFileName;
	String recordsData = null;
	//Row rowData = null;
	byte[] encodedOffset = null;

	public IndexItem(String indexFileName, int dataSerialNumber ,String recordsData, long offset) {
		this.indexFileName = indexFileName;
		this.dataSerialNumber = dataSerialNumber;
		this.recordsData = recordsData;
		encodedOffset = RecordsUtils.encodeIndex(dataSerialNumber, offset);
	}
	
	public byte[] getOffset() {
		return encodedOffset;
	}
	
	public int getFileIndex() {
		return dataSerialNumber;
	}

	public String getRecordsData() {
		return recordsData;
	}
	
	public String getIndexFileName(){
		return indexFileName;
	}

}
