package com.alibaba.middleware.disruptor;

import com.alibaba.middleware.tools.RecordsUtils;

public class RecordsEvent {

	//这里dataFileName+offset=Value
	int dataSerialNumber;
	String recordsData = null;
	//Row rowData = null;
	byte[] encodedOffset = null;
	
	public RecordsEvent() {
		
	}

	public void setData(String recordsData, int dataSerialNumber , long offset) {
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

}
