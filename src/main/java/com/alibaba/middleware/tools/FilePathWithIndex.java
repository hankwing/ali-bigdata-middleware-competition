package com.alibaba.middleware.tools;

/**
 * 存有文件的地址以及各个索引元数据的偏移地址(如果不为Null的话)
 * @author hankwing
 *
 */
public class FilePathWithIndex {

	public String filePath = null;
	public long orderIdIndex = 0;
	public long orderBuyerIdIndex = 0;
	public long orderGoodIdIndex = 0;
	public long buyerIdIndex = 0;
	public long goodIdIndex = 0;
	public long surrogateIndex = 0;
	
	public void setSurrogateIndex( long offset) {
		surrogateIndex = offset;
	}
	
	public long getSurrogateIndex() {
		return surrogateIndex;
	}
	
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
	
	public void setFilePath( String filePath) {
		this.filePath = filePath;
	}
	
	public String getFilePath() {
		return filePath;
	}
}
