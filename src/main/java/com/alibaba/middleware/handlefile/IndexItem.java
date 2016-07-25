package com.alibaba.middleware.handlefile;

import com.alibaba.middleware.tools.RecordsUtils;

public class IndexItem {

	//这里dataFileName+offset=Value
	int dataSerialNumber;
	int indexFileNumber;
	String recordsData = null;
	//Row rowData = null;
	byte[] encodedOffset = null;

	public IndexItem(int indexFileNumber, int dataSerialNumber ,String recordsData, long offset) {
		this.indexFileNumber = indexFileNumber;
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
	
	public int getIndexFileNumber(){
		return indexFileNumber;
	}

}
