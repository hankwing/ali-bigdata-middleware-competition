package com.alibaba.middleware.disruptor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.alibaba.middleware.cache.BucketCachePool;
import com.alibaba.middleware.cache.ConcurrentCache;
import com.alibaba.middleware.cache.SimpleCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.handlefile.DataFileMapping;
import com.alibaba.middleware.handlefile.IndexItem;
import com.alibaba.middleware.handlefile.WriteFile;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.ByteUtils;
import com.alibaba.middleware.tools.BytesKey;
import com.alibaba.middleware.tools.RecordsUtils;
import com.lmax.disruptor.RingBuffer;

/***
 * 卖家信息处理
 * 1）小文件合并
 * 2）索引项：文件编号+文件偏移量
 * 3）索引文件：索引项固定数目
 * @author legend
 *
 */
public class RecordsProducer{

	private int dataFileSerialNumber;
	private long offset = 0;
	private BufferedReader reader;
	private int threadIndex = 0;
	private RingBuffer<RecordsEvent> ringBuffer = null;
	private int nextLineByteLength = "\n".getBytes().length;
	
	public RecordsProducer( RingBuffer<RecordsEvent> ringBuffer) {
		this.ringBuffer = ringBuffer;
	}

	/**
	 * 处理每一行数据
	 * @param files
	 */
	public void handeFiles(Collection<String> files, 
			ConcurrentHashMap<Integer, LinkedBlockingQueue<RandomAccessFile>> handlersList,
			DataFileMapping fileMapping) {
		System.out.println("start order handling!");

		for (String file : files) {
			try {
				System.out.println("order file:" + file);
				File bf = new File(file);
				dataFileSerialNumber = fileMapping.addDataFileName(file);
				// 建立文件句柄
				LinkedBlockingQueue<RandomAccessFile> handlersQueue = 
						handlersList.get(dataFileSerialNumber);
				if( handlersQueue == null) {
					handlersQueue = new LinkedBlockingQueue<RandomAccessFile>();
					handlersList.put(dataFileSerialNumber, handlersQueue);
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
						long sequence = ringBuffer.next();  // Grab the next sequence
				        try
				        {
				            RecordsEvent event = ringBuffer.get(sequence); // Get the entry in the Disruptor
				            // for the sequence
				            event.setData(record, dataFileSerialNumber, offset);
				        }
				        finally
				        {
				            ringBuffer.publish(sequence);
				            offset = offset + record.getBytes().length + nextLineByteLength;
				        }
						record = reader.readLine();
					}
					// 新文件 offset要置0
					offset = 0;
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		// 继续发送多个结束标识
		for( int i = 0; i < 1; i++) {
			long sequence = ringBuffer.next();
			try
	        {
	            RecordsEvent event = ringBuffer.get(sequence); // Get the entry in the Disruptor
	            // for the sequence
	            event.setData(null, dataFileSerialNumber, offset);
	        }
	        finally
	        {
	            ringBuffer.publish(sequence);
	        }
		}
		
		System.out.println("end order reading!");
	}

	
}
