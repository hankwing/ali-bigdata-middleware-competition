package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.cache.ConcurrentCache;
import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.conf.RaceConfig.IdIndexType;
import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.handlefile.BuyerHandler.BuyerIndexConstructor;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.ByteUtils;
import com.alibaba.middleware.tools.BytesKey;
import com.alibaba.middleware.tools.FilePathWithIndex;
import com.alibaba.middleware.tools.RecordsUtils;

/***
 * 商品信息表处理：
 * 1）小文件合并
 * 2）索引项：文件编号+文件偏移量
 * 3）索引文件：索引项固定数目
 * @author legend
 *
 */
public class GoodHandler{

	WriteFile goodfile;
	SmallFileWriter smallFileWriter;
	//文件映射，文件编号
	DataFileMapping goodFileMapping;
	DataFileMapping goodIndexMapping;				// 存buyer表里索引的文件信息
	int dataFileSerialNumber;

	BufferedReader reader;
	LinkedBlockingQueue<IndexItem> indexQueue;

	//DiskHashTable<String, Long> goodIdSurrKeyIndex = null;
	ConcurrentHashMap<Integer, DiskHashTable<BytesKey, byte[]>> goodIdIndexList = null;
	HashSet<String> goodAttrList = null;
	int threadIndex = 0;
	CountDownLatch latch = null;
	private ConcurrentCache rowCache = null;
	public ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> goodHandlersList = null;
	private OrderSystemImpl system = null;

	public double MEG = Math.pow(1024, 2);
	List<String> smallFiles = new ArrayList<String>();

	public GoodHandler(OrderSystemImpl systemImpl ,int threadIndex,CountDownLatch latch) {
		this.system = systemImpl;
		rowCache = ConcurrentCache.getInstance();
		this.latch = latch;
		this.goodAttrList = systemImpl.goodAttrList;
		//this.goodIdSurrKeyIndex = goodIdSurrKeyIndex;
		this.goodIdIndexList = systemImpl.goodIdIndexList;
		this.threadIndex = threadIndex;
		this.goodHandlersList = systemImpl.goodHandlersList;
		indexQueue = new LinkedBlockingQueue<IndexItem>(RaceConfig.QueueNumber);
		goodfile = new WriteFile(new ArrayList<LinkedBlockingQueue<IndexItem>>(){{add(indexQueue);}},
				RaceConfig.storeFolders[threadIndex], 
				RaceConfig.goodFileNamePrex, (int) RaceConfig.maxIndexFileCapacity);

		this.goodFileMapping = systemImpl.goodFileMapping;
		this.goodIndexMapping = systemImpl.goodIndexMapping;
	}

	public void HandleGoodFiles(List<String> files){
		System.out.println("start good handling!");
		new Thread(new GoodIndexConstructor()).start();					// 同时开启建索引线程
		for (String file : files) {
			System.out.println("good file:" + file);
			File bf = new File(file);
			/*if (bf.length() < RaceConfig.smallFileSizeThreshold) {
				System.out.println("small good file:" + file);
				smallFiles.add(file);

			}else{*/
				try {
					// 属于大文件
					dataFileSerialNumber = goodFileMapping.addDataFileName(file);
					reader = new BufferedReader(new FileReader(file));
					// 建立文件句柄
					LinkedBlockingQueue<RandomAccessFile> handlersQueue = 
							goodHandlersList.get(dataFileSerialNumber);
					if( handlersQueue == null) {
						handlersQueue = new LinkedBlockingQueue<RandomAccessFile>();
						goodHandlersList.put(dataFileSerialNumber, handlersQueue);
					}

					for( int i = 0; i < RaceConfig.fileHandleNumber ; i++) {
						handlersQueue.add(new RandomAccessFile(file, "r"));
					}
					
					String record = reader.readLine();
					while (record != null) {
						//Utils.getAttrsFromRecords(goodAttrList, record);
						goodfile.writeLine(dataFileSerialNumber, record);
						record = reader.readLine();
					}
					reader.close();
					
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			//}
		}

		smallFileWriter = new SmallFileWriter(
				goodHandlersList, goodFileMapping,
				new ArrayList<LinkedBlockingQueue<IndexItem>>(){{add(indexQueue);}}, 
				RaceConfig.storeFolders[threadIndex],
				RaceConfig.goodFileNamePrex);
			//开始处理小文件
		for(String smallfile:smallFiles){

			try {
				// 下面开始处理小文件
				
				reader = new BufferedReader(new FileReader(smallfile));
				String record = reader.readLine();
				while (record != null) {
					//Utils.getAttrsFromRecords(buyerAttrList, record);
					smallFileWriter.writeLine(record);
					record = reader.readLine();
				}
				reader.close();
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		smallFileWriter.writeLine(null);
		smallFileWriter.closeFile();
		
		System.out.println("end good handling!");
	}

	// good表的建索引线程  需要建的索引包括：代理键索引和goodId的索引
	public class GoodIndexConstructor implements Runnable {
		
		int fileIndex = 0;
		String indexFileName = null;
		DiskHashTable<BytesKey, byte[]> goodIdHashTable = null;
		boolean isEnd = false;
		HashSet<String> tempAttrList = new HashSet<String>();
		//long surrKey = 1;

		public GoodIndexConstructor( ) {

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
							indexFileName = record.getIndexFileName();
							String diskFileName = RaceConfig.storeFolders[(threadIndex + 2) % 3]
									+ indexFileName.replace("/", "_").replace("//", "_");
							System.out.println("create good index:" + diskFileName);
							fileIndex = goodIndexMapping.addDataFileName(indexFileName);
							goodIdHashTable = new DiskHashTable<BytesKey,byte[]>(system,
									diskFileName + RaceConfig.goodIndexFileSuffix, byte[].class,
									DirectMemoryType.GoodIdSegment);

						}
						else {
							// 保存当前goodId的索引  并写入索引List
							//goodIdHashTable.writeAllBuckets();
							//smallFile.setGoodIdIndex(0);
							goodIdIndexList.put(fileIndex, goodIdHashTable);	
							indexFileName = record.getIndexFileName();
							// 将三张表的索引写到不同的disk里去
							String diskFileName = RaceConfig.storeFolders[(threadIndex + 2) % 3]
									+ indexFileName.replace("/", "_").replace("//", "_");
							System.out.println("create good index:" + diskFileName);
							fileIndex = goodIndexMapping.addDataFileName(indexFileName);
							goodIdHashTable = new DiskHashTable<BytesKey,byte[]>(system,
									diskFileName + RaceConfig.goodIndexFileSuffix, byte[].class,
									DirectMemoryType.GoodIdSegment);


						}
					}
					//Row rowData = Row.createKVMapFromLine(record.getRecordsData());	
					//tempAttrList.addAll(rowData.keySet());
					//String goodid = rowData.getKV(RaceConfig.goodId).valueAsString();
					//Integer goodIdHashCode = goodid.hashCode();
					// 放入缓冲区
					//rowCache.putInCache(goodIdHashCode, record.getRecordsData(), TableName.GoodTable);
					goodIdHashTable.put(new BytesKey(RecordsUtils.getValueFromLineWithKeyList(
							record.getRecordsData(),RaceConfig.goodId, tempAttrList).getBytes()), 
							record.getOffset());
					//surrKey ++;
				}
				else if(isEnd ) {
					// 说明队列为空
					// 将代理键索引写出去  并保存相应数据   将gooderid索引写出去  并保存相应数据
					//goodIdSurrKeyFile.setFilePath(RaceConfig.goodSurrFileName);
					//goodIdSurrKeyFile.setSurrogateIndex(goodIdSurrKeyIndex.writeAllBuckets());
					synchronized (goodAttrList) {
						goodAttrList.addAll(tempAttrList);
					}
					//goodIdHashTable.writeAllBuckets();

					//smallFile.setGoodIdIndex(0);
					BucketCachePool.getInstance().removeAllBucket();
					goodIdIndexList.put(fileIndex, goodIdHashTable);
					latch.countDown();
					break;
				}

			}


		}
	}
}
