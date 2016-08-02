package com.alibaba.middleware.tools;

import java.io.FileNotFoundException;

/**
 * 存有文件的地址以及各个索引元数据的偏移地址(如果不为Null的话)
 * @author hankwing
 *
 */
public class FilePathWithIndex {

	public int filePathIndex = 0;
	public long orderIdIndex = 0;
	public long orderBuyerIdIndex = 0;
	public long orderGoodIdIndex = 0;
	public long buyerIdIndex = 0;
	public long goodIdIndex = 0;
	//public RandomAccessFile file = null
	
	public void setOrderIdIndex( long offset) {
		orderIdIndex = offset;
	}
	
	public long getOrderIdIndex() {
		return orderIdIndex;
	}
	
	public void setOrderBuyerIdIndex( long offset) {
		orderBuyerIdIndex = offset;
	}
	
	public long getOrderBuyerIdIndex() {
		return orderBuyerIdIndex;
	}
	
	public void setOrderGoodIdIndex( long offset) {
		orderGoodIdIndex = offset;
	}
	
	public long getOrderGoodIdIndex() {
		return orderGoodIdIndex;
	}
	
	public void setBuyerIdIndex( long offset) {
		buyerIdIndex = offset;
	}
	
	public long getBuyerIdIndex() {
		return buyerIdIndex;
	}
	
	public void setGoodIdIndex( long offset) {
		goodIdIndex = offset;
	}
	
	public long getGoodIdIndex() {
		return goodIdIndex;
	}
	
	/**
	 * 在设置文件的时候同时设置随机读的对象
	 * @param filePath
	 */
	public void setFilePathIndex( int filePathIndex) {
		this.filePathIndex = filePathIndex;
	}
	
	/*public RandomAccessFile getAccessFile() {
		return file;
	}*/
	
	public int getFilePath() {
		return filePathIndex;
	}
}
