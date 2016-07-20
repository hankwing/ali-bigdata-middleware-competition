package com.alibaba.middleware.handlefile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IndexType;

public class WriteFile {

	/**
	 * 文件最多纪录数为 MAX_LINES
	 * 文件的偏移量为 offset
	 * 文件纪录的计数 count
	 */

	private long offset;
	private String fileName;

	private LinkedBlockingQueue<IndexItem> indexQueue = null;

	/**
	 * 写数据到小文件里  并且将数据放到缓冲区里  缓冲区里保存文件名+数据
	 * @param indexQueue
	 * @param path	文件路径
	 * @param name	文件名
	 * @param maxLines	每个小文件最大记录数
	 */	
	public WriteFile(LinkedBlockingQueue<IndexItem> indexQueue, String path,String name, long maxLines) {
		this.offset = 0;
		this.indexQueue = indexQueue;

		//如果文件夹不存在则创建文件夹
		File file = new File(path);
		if (!file.exists()) {
			file.mkdirs();
		}

	}

	public void writeLine(String file,String line, IndexType type){
		try {
			if (fileName == null || !fileName.equals(file)) {
				offset = 0;
			}
			// 将数据放入队列中 供建索引的线程建索引
			indexQueue.put(new IndexItem(file, line, offset, type));
			offset = offset + line.length() + 1;
			fileName = file;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public long getOffset() {
		return offset;
	}

}
