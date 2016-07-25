package com.alibaba.middleware.handlefile;

import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.tools.RecordsUtils;

public class IndexItem {

	//这里dataFileName+offset=Value
	public int dataSerialNumber;
	public int indexFileNumber;
	public long orderId;
	public int buyerId;
	public int goodId;
	//String recordsData = null;
	//Row rowData = null;
	byte[] encodedOffset = null;
	// 适合于good和buyer表的构造函数
	public IndexItem(int indexFileNumber,int dataSerialNumber,  int id , long offset, IdName idName) {
		switch( idName) {
		case BuyerId:
			this.buyerId =id;
			break;
		case GoodId:
			this.goodId = id;
			break;
		default:
			break;
		}
		this.indexFileNumber = indexFileNumber;
		this.dataSerialNumber = dataSerialNumber;
		//this.recordsData = recordsData;
		encodedOffset = RecordsUtils.encodeIndex(dataSerialNumber, offset);
	}
	
	/**
	 * 用于order表的构造函数  传入三个id
	 * @param dataSerialNumber
	 * @param indexFileNumber
	 * @param id
	 * @param offset
	 * @param idName
	 */
	public IndexItem( int indexFileNumber, int dataSerialNumber,
			long orderId ,int buyerId, int goodId, long offset) {
		this.orderId = orderId;
		this.buyerId = buyerId;
		this.goodId = goodId;
		this.indexFileNumber = indexFileNumber;
		this.dataSerialNumber = dataSerialNumber;
		//this.recordsData = recordsData;
		encodedOffset = RecordsUtils.encodeIndex(dataSerialNumber, offset);
	}
	
	public byte[] getOffset() {
		return encodedOffset;
	}
	
	public int getFileIndex() {
		return dataSerialNumber;
	}

	/*public String getRecordsData() {
		return recordsData;
	}*/
	
	public int getIndexFileNumber(){
		return indexFileNumber;
	}

}
