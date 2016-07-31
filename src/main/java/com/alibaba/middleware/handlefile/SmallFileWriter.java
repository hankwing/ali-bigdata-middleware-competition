package com.alibaba.middleware.handlefile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.tools.ByteUtils;

/***
 * 融合小文件，生成一定数目记录的文件，根据合并后的文件创建索引
 * @author legend
 *
 */
public class SmallFileWriter {

	/**
	 * 文件最多纪录数为 MAX_LINES
	 * 文件的偏移量为 offset
	 * 文件纪录的计数 count
	 */
	private long MAX_LINES = 0;
	private long offset;
	private int count;
	/**
	 * 写出缓冲区 writer
	 * 源数据文件前缀 dataFilePerfix
	 * 源数据文件名 dataFileName
	 * 索引文件名 indexFileName
	 */
	private BufferedWriter writer;
	private String dataFilePerfix;
	private String dataFileName;
	private int dataFileNumber;
	private ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> fileHandlersList;

	//建立索引
	private List<LinkedBlockingQueue<IndexItem>> indexQueues = null;
	private int nextLineByteLength = 0;
	
	//数据文件映射
	private DataFileMapping dataFileMapping;
	private int dataFileSerialNumber;
	
	private int interval;

	public SmallFileWriter(
			ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> fileHandlersList,
			DataFileMapping dataFileMapping,
			List<LinkedBlockingQueue<IndexItem>> indexQueues, 
			String path,String name) {
		this.offset = 0;
		this.count = 0;

		this.indexQueues = indexQueues;
		this.MAX_LINES = RaceConfig.singleFileMaxLines;
		this.fileHandlersList = fileHandlersList;
		this.dataFileMapping = dataFileMapping;
		
		this.interval = 3;
		
		if (name.equals(RaceConfig.buyerFileNamePrex)) {
			this.dataFileNumber = 0;
		}
		if (name.equals(RaceConfig.goodFileNamePrex)) {
			this.dataFileNumber = 1;
		}
		if (name.equals(RaceConfig.goodFileNamePrex)) {
			this.dataFileNumber = 2;
		}
		
		nextLineByteLength = "\n".getBytes().length;

		//如果文件夹不存在则创建文件夹
		File file = new File(path);
		if (!file.exists()) {
			file.mkdirs();
		}

		dataFilePerfix = new String(path + name + "smallfile_");
		try {
			dataFileName = dataFilePerfix + String.valueOf(dataFileNumber);
			this.writer = new BufferedWriter(new FileWriter(dataFileName));
			dataFileSerialNumber = dataFileMapping.addDataFileName(dataFileName);
			
			LinkedBlockingQueue<RandomAccessFile> handlersQueue = fileHandlersList.get(dataFileSerialNumber);
			if( handlersQueue == null) {
				handlersQueue = new LinkedBlockingQueue<RandomAccessFile>();
				fileHandlersList.put(dataFileSerialNumber, handlersQueue);
			}

			for( int i = 0; i < RaceConfig.fileHandleNumber ; i++) {
				handlersQueue.add(new RandomAccessFile(dataFileName, "r"));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/***
	 * 合并小文件
	 * @param file
	 * @param line
	 * @param tableType
	 */
	public void writeLine(String line){
		try {

			//当记录达到一定数目进行创建新的源数据文件,即将小文件进行合并
			if (count == MAX_LINES) {
				writer.close();
				//创建新的文件
				dataFileNumber = dataFileNumber + interval;
				dataFileName = dataFilePerfix + String.valueOf(dataFileNumber);

				dataFileSerialNumber = dataFileMapping.addDataFileName(dataFileName);
				writer = new BufferedWriter(new FileWriter(dataFileName));
				offset = 0;
				count = 0;
				// 加入文件句柄缓冲池
				LinkedBlockingQueue<RandomAccessFile> handlersQueue = fileHandlersList.get(dataFileSerialNumber);
				if( handlersQueue == null) {
					handlersQueue = new LinkedBlockingQueue<RandomAccessFile>();
					fileHandlersList.put(dataFileSerialNumber, handlersQueue);
				}

				for( int i = 0; i < RaceConfig.fileHandleNumber ; i++) {
					handlersQueue.add(new RandomAccessFile(dataFileName, "r"));
				}
				
			}
			if (line!=null) {
				writer.write(line+"\n");
				IndexItem sendItem = new IndexItem(dataFileName, dataFileSerialNumber, line, offset);
				for(LinkedBlockingQueue<IndexItem> queue : indexQueues) {
					try {
						queue.put(sendItem);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				offset = offset + line.getBytes().length + nextLineByteLength;
				count++;
			}
			else {
				writer.flush();
				writer.close();
				// 还需要发送结束IndexItem
				for(LinkedBlockingQueue<IndexItem> queue : indexQueues) {
					try {
						queue.put(new IndexItem(null, dataFileSerialNumber, line, offset));
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
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

}
