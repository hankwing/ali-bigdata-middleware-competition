package com.alibaba.middleware.handlefile;


import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.race.Row;

public class WriteFile {

	/**
	 * 文件最多纪录数为 MAX_LINES
	 * 文件的偏移量为 offset
	 * 文件纪录的计数 count
	 */
	private long MAX_LINES = RaceConfig.smallFileCapacity;
	private long offset;
	private int count;
	private int fileNum;
	
	private String dataFileName;
	private String indexFileName;
	private LinkedBlockingQueue<IndexItem> indexQueue = null;
	private SimpleCache rowCache = null;

	/**
	 * 写数据到小文件里  并且将数据放到缓冲区里  缓冲区里保存文件名+数据
	 * @param indexQueue
	 * @param path	文件路径
	 * @param name	文件名
	 * @param maxLines	每个小文件最大记录数
	 */	
	public WriteFile(LinkedBlockingQueue<IndexItem> indexQueue, String path,String name, long maxLines) {
		this.offset = 0;
		this.count = 0;
		this.fileNum = 0;
		this.MAX_LINES = maxLines;
		this.indexQueue = indexQueue;
		
		dataFileName = null;
		indexFileName = null;
		rowCache = SimpleCache.getInstance();

		//如果文件夹不存在则创建文件夹
		File file = new File(path);
		if (!file.exists()) {
			file.mkdirs();
		}
	}

	public void writeLine(String dataFileName, String line, TableName type){
		try {
			/***
			 * 索引文件为空时创建新的索引文件
			 * 数据文件变化时创建新的索引文件
			 */
			if (indexFileName == null || !this.dataFileName.equals(dataFileName)) {
				fileNum = 0;
				indexFileName = dataFileName + "_" + fileNum;
				fileNum++;
				offset = 0;
				count = 0;
				this.dataFileName = dataFileName;
			}
			
			if (count == MAX_LINES) {
				indexFileName = dataFileName + "_" + fileNum;
				fileNum++;
				offset = 0;
				count = 0;
			}
			// 将数据放入队列中 供建索引的线程建索引
			indexQueue.put(new IndexItem(indexFileName, dataFileName, 
					Row.createKVMapFromLine(line), offset));
			if(line != null ) {
				// 放入缓冲区中
				rowCache.putInCache(dataFileName.hashCode() + offset
					, line, type);
			}

			String writeLine = line + "\n";
			offset = offset + writeLine.getBytes().length;
			count++;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public long getOffset() {
		return offset;
	}
	
	public String getIndexFileName() {
		return indexFileName;
	}
	public String getDataFileName() {
		return dataFileName;
	}
}
