package com.alibaba.middleware.handlefile;


import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdName;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.tools.RecordsUtils;

/***
 * 普通文件处理，
 * 1）将记录生成对应的索引
 * 2）一定数量的索引生成索引文件
 * @author legend
 *
 */
public class WriteFile {

	/**
	 * 文件最多纪录数为 MAX_LINES
	 * 文件的偏移量为 offset
	 * 文件纪录的计数 count
	 */
	private long MAX_LINES = 0;
	private long offset;
	private int count;
	private int oldDataFileSerialNumber = 0;
	private IdName idName;
	private HashSet<String> keyList;				//全局的key的hashset
	private HashSet<String> tempKeyList;			// 本线程的key的hashset
	
	/**
	 * 索引文件前缀 indexFilePrefix
	 * 索引文件名 indexFileName
	 * 索引文件编号 indexFileNumber
	 */

	//private String indexFilePrefix;
	//private String indexFileName;
//	private String indexFilePrefix;
//	private String indexFileName;

	private int indexFileNumber;
	private List<LinkedBlockingQueue<IndexItem>> indexQueues = null;
	//private SimpleCache rowCache = null;
	private int nextLineByteLength = 0;

	/**
	 * 写数据到小文件里  并且将数据放到缓冲区里  缓冲区里保存文件名+数据
	 * @param indexQueue
	 * @param path	文件路径
	 * @param name	文件名
	 * @param maxLines	每个小文件最大记录数
	 */	
	public WriteFile(List<LinkedBlockingQueue<IndexItem>> indexQueues, 
			String path, long maxLines , IdName idName, HashSet<String> keyList) {
		this.offset = 0;
		this.count = 0;
		this.indexFileNumber = -1;	//	用于判断初始
		this.MAX_LINES = maxLines;
		this.indexQueues = indexQueues;
		nextLineByteLength = "\n".getBytes().length;
		this.idName = idName;
		this.keyList = keyList;
		tempKeyList = new HashSet<String>();
		//索引文件地址前缀
		//indexFilePrefix = new String(path + name);
		//indexFileName = null;
		//rowCache = SimpleCache.getInstance();

		//如果文件夹不存在则创建文件夹
		File file = new File(path);
		if (!file.exists()) {
			file.mkdirs();
		}
	}

	/***
	 * 生成索引项，其中包括，源数据文件名，数据文件编号
	 * @param dataFileName
	 * @param dataFileSerialNumber
	 * @param line
	 * @param tableType
	 */
	public void writeLine(int dataFileSerialNumber, String line, TableName tableType){
		try {
			/***
			 * 索引文件为空时创建新的索引文件
			 */
			if (indexFileNumber == -1) {
				oldDataFileSerialNumber = dataFileSerialNumber;
				indexFileNumber = 0;
				offset = 0;
				count = 0;
			}
			
			if( oldDataFileSerialNumber != dataFileSerialNumber) {
				// 说明是不同的文件了 这时候要清零offset
				oldDataFileSerialNumber = dataFileSerialNumber;
				offset = 0;
			}
			
			if (count == MAX_LINES) {
				indexFileNumber++;
				//indexFileName = indexFilePrefix + indexFileNumber;
				//	indexFileName = indexFilePrefix + indexFileNumber;
				//indexFileName = new StringBuilder();
				//indexFileName.append(indexFilePrefix).append(indexFileNumber);
				count = 0;
			}
			IndexItem sendItem = null;
			// 获得数据中的主键
			switch( idName) {
			case OrderId:
				sendItem = new IndexItem(indexFileNumber, dataFileSerialNumber,
						Long.parseLong(RecordsUtils.getValueFromLine(line, RaceConfig.orderId)),
						RecordsUtils.getValueFromLine(line, RaceConfig.buyerId).hashCode(),
						RecordsUtils.getValueFromLine(line, RaceConfig.goodId).hashCode(),offset);
				break;
			case BuyerId:
				sendItem = new IndexItem(indexFileNumber, dataFileSerialNumber, 
						RecordsUtils.getValueFromLineWithKeyList(line, RaceConfig.buyerId, tempKeyList).hashCode(),
						offset,IdName.BuyerId);
				break;
			case GoodId:
				sendItem = new IndexItem(indexFileNumber, dataFileSerialNumber, 
						RecordsUtils.getValueFromLineWithKeyList(line, RaceConfig.goodId, tempKeyList).hashCode(),
						offset,IdName.GoodId);
				break;
			}
			// 将数据放入队列中 供建索引的线程建索引
			
			for(LinkedBlockingQueue<IndexItem> queue : indexQueues) {
				queue.put(sendItem);
			}
			
			if(line != null ) {
				offset = offset + line.getBytes().length + nextLineByteLength;
				count++;
			}
			else {
				// 读文件结束了
				synchronized(keyList) {
					keyList.addAll(tempKeyList);
				}
			}
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public long getOffset() {
		return offset;
	}
	
	public int getIndexFileNumber() {
		return indexFileNumber;

	}
}
