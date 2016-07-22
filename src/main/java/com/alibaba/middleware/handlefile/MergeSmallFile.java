package com.alibaba.middleware.handlefile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.conf.RaceConfig.TableName;

import javafx.scene.chart.PieChart.Data;

public class MergeSmallFile {

	/**
	 * 文件最多纪录数为 MAX_LINES
	 * 文件的偏移量为 offset
	 * 文件纪录的计数 count
	 */
	
	private int MAX_LINES = 10000000;
	private long offset;
	private int count;

	private BufferedWriter writer;
	private String filePerfix;
	private String dataFileName;
	private String indexFileName;
	private int fileNum;

	//建立索引
	private List<LinkedBlockingQueue<IndexItem>> indexQueues = null;
	private int nextLineByteLength = 0;
	
	//数据文件映射
	private DataFileMapping dataFileMapping;
	int dataFileSerialNumber = 0;

	public MergeSmallFile(
			DataFileMapping dataFileMapping,
			List<LinkedBlockingQueue<IndexItem>> indexQueues, 
			String path,String name, int maxLines) {
		this.offset = 0;
		this.count = 0;
		this.fileNum = 0;
		this.indexQueues = indexQueues;
		this.MAX_LINES = maxLines;
		
		this.dataFileMapping = dataFileMapping;

		nextLineByteLength = "\n".getBytes().length;

		//如果文件夹不存在则创建文件夹
		File file = new File(path);
		if (!file.exists()) {
			file.mkdirs();
		}

		filePerfix = new String(path + name);
		try {
			dataFileName = filePerfix + String.valueOf(fileNum);
			indexFileName = dataFileName + "_";
			this.writer = new BufferedWriter(new FileWriter(dataFileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void writeLine(String dataFileName, String line, TableName tableType){
		try {

			//当记录达到一定数目进行创建新的源数据文件
			if (count == MAX_LINES) {
				writer.close();
				//创建新的文件
				fileNum++;
				dataFileName = filePerfix + String.valueOf(fileNum);
				
				//添加到文件映射中
				dataFileMapping.addDataFile(dataFileName);
				dataFileSerialNumber = dataFileMapping.getDataFileSerialNumber();
				
				indexFileName = dataFileName+"_";
				writer = new BufferedWriter(new FileWriter(dataFileName));
				offset = 0;
				count = 0;
			}
			if (line!=null) {
				writer.write(line+"\n");
				offset = offset + line.length() + nextLineByteLength;
				for(LinkedBlockingQueue<IndexItem> queue : indexQueues) {
					try {
						queue.put(new IndexItem(indexFileName, dataFileName, dataFileSerialNumber, line, offset));
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				count++;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeFile(){
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public long getOffset() {
		return offset;
	}

	/**
	 * 获得源数据文件名称
	 * @return
	 */
	public String getDataFileName() {
		return dataFileName;
	}

	/***
	 * 获得索引文件名称
	 * @return
	 */
	public String getIndexFileName(){
		return indexFileName;
	}

}
