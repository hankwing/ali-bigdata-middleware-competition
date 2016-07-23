package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.IdIndexType;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.FilePathWithIndex;

import javafx.scene.chart.PieChart.Data;

/***
 * 卖家信息处理
 * 1）小文件合并
 * 2）索引项：文件编号+文件偏移量
 * 3）索引文件：索引项固定数目
 * @author legend
 *
 */
public class BuyerHandler{

	//普通文件、合并小文件
	WriteFile buyerfile;
	SmallFileWriter smallFileWriter;
	//文件编号映射，文件序列号
	DataFileMapping buyerFileMapping;
	int dataFileSerialNumber;
	BufferedReader reader;
	//阻塞队列用于存索引
	LinkedBlockingQueue<IndexItem> indexQueue;
	//DiskHashTable<String, Long> buyerIdSurrKeyIndex = null;
	ConcurrentHashMap<Integer, DiskHashTable<Integer, List<byte[]>>> buyerIdIndexList = null;
	HashSet<String> buyerAttrList = null;
	int threadIndex = 0;
	CountDownLatch latch = null;
	SimpleCache rowCache = null;
	ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> buyerHandlersList = null;
	List<String> smallFiles = new ArrayList<String>();

	public BuyerHandler( OrderSystemImpl systemImpl, int threadIndex, CountDownLatch latch) {
		rowCache = SimpleCache.getInstance();
		this.latch = latch;
		this.buyerAttrList = systemImpl.buyerAttrList;
		//this.buyerIdSurrKeyIndex = buyerIdSurrKeyIndex;
		this.buyerIdIndexList = systemImpl.buyerIdIndexList;
		this.threadIndex = threadIndex;
		this.buyerHandlersList = systemImpl.buyerHandlersList;
		indexQueue = new LinkedBlockingQueue<IndexItem>(RaceConfig.QueueNumber);
		buyerfile = new WriteFile(new ArrayList<LinkedBlockingQueue<IndexItem>>(){{add(indexQueue);}}, 
				RaceConfig.storeFolders[threadIndex],
				RaceConfig.buyerFileNamePrex, (int) RaceConfig.smallFileCapacity);
		
		//文件映射
		this.buyerFileMapping =  systemImpl.buyerFileMapping;
	}

	/**
	 * 处理每一行数据
	 * @param files
	 */
	public void handeBuyerFiles(List<String> files){
		System.out.println("start buyer handling!");
		new Thread(new BuyerIndexConstructor( )).start();					// 同时开启建索引线程

		for (String file : files) {
			try {
				File bf = new File(file);
				if (bf.length() < RaceConfig.smallFileSizeThreathod) {
					// 属于小文件
					smallFiles.add(file);
				}else {
					// 属于大文件
					dataFileSerialNumber = buyerFileMapping.addDataFileName(file);
					// 建立文件句柄
					LinkedBlockingQueue<RandomAccessFile> handlersQueue = buyerHandlersList.get(file);
					if( handlersQueue == null) {
						handlersQueue = new LinkedBlockingQueue<RandomAccessFile>();
						buyerHandlersList.put(dataFileSerialNumber, handlersQueue);
					}
					for( int i = 0; i < RaceConfig.fileHandleNumber ; i++) {
						handlersQueue.add(new RandomAccessFile(file, "r"));
					}
					
					reader = new BufferedReader(new FileReader(bf));
	
					String record = null;
					try {
						record = reader.readLine();
						while (record != null) {
							//Utils.getAttrsFromRecords(buyerAttrList, record);
							buyerfile.writeLine(dataFileSerialNumber, record, TableName.BuyerTable);
							record = reader.readLine();
						}
						reader.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				}
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		// 下面开始处理小文件
		smallFileWriter = new SmallFileWriter(
				buyerHandlersList, buyerFileMapping,
				new ArrayList<LinkedBlockingQueue<IndexItem>>(){{add(indexQueue);}}, 
				RaceConfig.storeFolders[threadIndex],
				RaceConfig.buyerFileNamePrex);

		//处理小文件，合并
		for(String smallfile:smallFiles){
			try {
				reader = new BufferedReader(new FileReader(smallfile));
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			String record = null;
			try {
				record = reader.readLine();
				while (record != null) {
					//Utils.getAttrsFromRecords(buyerAttrList, record);
					smallFileWriter.writeLine(record, TableName.BuyerTable);
					record = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		smallFileWriter.writeLine(null, TableName.BuyerTable);
		smallFileWriter.closeFile();
		System.out.println("end buyer handling!");
	}

	// buyer表的建索引线程  需要建的索引包括：代理键索引和buyerId的索引
	public class BuyerIndexConstructor implements Runnable {

		String indexFileName = null;
		DiskHashTable<Integer, List<byte[]>> buyerIdHashTable = null;
		boolean isEnd = false;
		HashSet<String> tempAttrList = new HashSet<String>();
		int fileIndex = 0;
		//long surrKey = 1;

		public BuyerIndexConstructor( ) {

		}

		public void run() {

			while( true) {
				IndexItem record = indexQueue.poll();
				
				if( record != null ) {
					if( record.getRecordsData() == null) {
						isEnd = true;
						continue;
					}

					if( !record.getIndexFileName().equals(indexFileName)) {
						if( indexFileName == null) {
							// 第一次建立索引文件
							fileIndex = record.dataSerialNumber;
							indexFileName = record.getIndexFileName();
							buyerIdHashTable = new DiskHashTable<Integer,List<byte[]>>(
									indexFileName + RaceConfig.buyerIndexFileSuffix, List.class);

						}
						else {
							// 保存当前buyerId的索引  并写入索引List
							buyerIdHashTable.writeAllBuckets();
							//smallFile.setBuyerIdIndex(0);
							buyerIdIndexList.put(fileIndex, buyerIdHashTable);
							fileIndex = record.dataSerialNumber;
							indexFileName = record.getIndexFileName();
							buyerIdHashTable = new DiskHashTable<Integer,List<byte[]>>(
									indexFileName + RaceConfig.buyerIndexFileSuffix, List.class);

						}
					}

					Row rowData = Row.createKVMapFromLine(record.getRecordsData());
					tempAttrList.addAll(rowData.keySet());			// 添加属性
					String buyerid = rowData.getKV(RaceConfig.buyerId).valueAsString();
					Integer buyerIdHashCode = buyerid.hashCode();
					//rowCache.putInCache(buyerIdHashCode, record.getRecordsData(), TableName.BuyerTable);
					//buyerIdSurrKeyIndex.put(buyerid, surrKey);					// 建立代理键索引
					buyerIdHashTable.put(buyerIdHashCode, record.getOffset());
					//surrKey ++;
				}
				else if(isEnd ) {
					// 说明队列为空
					// 将代理键索引写出去  并保存相应数据   将buyerid索引写出去  并保存相应数据
					//buyerIdSurrKeyFile.setFilePath(RaceConfig.buyerSurrFileName);
					//buyerIdSurrKeyFile.setSurrogateIndex(buyerIdSurrKeyIndex.writeAllBuckets());
					synchronized (buyerAttrList) {
						buyerAttrList.addAll(tempAttrList);
					}
					BucketCachePool.getInstance().removeAllBucket();

					buyerIdHashTable.writeAllBuckets();
					buyerIdIndexList.put(fileIndex, buyerIdHashTable);
					latch.countDown();
					break;
				}

			}
		}
	}
}
