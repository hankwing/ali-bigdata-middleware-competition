package com.alibaba.middleware.handlefile;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class IndexItem {

	//这里dataFileName+offset=Value
	String dataFileName;
	int dataSerialNumber;
	String indexFileName;
	String recordsData = null;
	//Row rowData = null;
	long offset = 0;

	public IndexItem(String indexFileName, String dataFileName, int dataSerialNumber ,String recordsData, long offset) {
		this.indexFileName = indexFileName;
		this.dataFileName = dataFileName;
		this.dataSerialNumber = dataSerialNumber;
		this.recordsData = recordsData;
		this.offset = offset;
	}
	
	/**
	 * 
	 * @param dataFileMapping
	 * @param indexBytes
	 */
	public IndexItem(HashMap<Integer, String> dataFileMapping, byte[] indexBytes){
		ByteBuffer buffer = ByteBuffer.wrap(indexBytes);
		dataSerialNumber = buffer.getInt();
		offset = buffer.getLong();
		dataFileName = dataFileMapping.get(dataSerialNumber);
		
		indexFileName = null;
		recordsData = null;
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
	
	public byte[] getIndexBytes(){
		ByteBuffer buffer = ByteBuffer.allocate(12);
		//放入源数据文件编号
		buffer.putInt(dataSerialNumber);
		buffer.putLong(offset);
		buffer.clear();
		return buffer.array();
	}

}
