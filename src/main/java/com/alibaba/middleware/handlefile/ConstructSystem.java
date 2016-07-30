package com.alibaba.middleware.handlefile;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.tools.FilePathWithIndex;

/**
 * 读三种类型的文件 写入小文件 并由单独线程处理数据
 * 
 * @author daliang
 *
 */
public class ConstructSystem {

	
	private OrderSystemImpl systemImpl = null;
	
	class BuyerRun implements Runnable {
		CountDownLatch countDownLatch;
		List<String> files;
		int threadIndex = 0;

		public BuyerRun(CountDownLatch countDownLatch, List<String> files, int i) {
			this.countDownLatch = countDownLatch;
			this.files = files;
			threadIndex = i;
		}

		public void run() {
			// TODO Auto-generated method stub
			if( !files.isEmpty()) {
				BuyerHandler buyerHandler = new BuyerHandler(systemImpl, threadIndex, countDownLatch);
				buyerHandler.handeBuyerFiles(files);
			}
			else {
				countDownLatch.countDown();
			}
			
			//countDownLatch.countDown();
		}
	}

	class GoodRun implements Runnable {
		CountDownLatch countDownLatch;
		List<String> files;
		int threadIndex = 0;
		
		public GoodRun(CountDownLatch countDownLatch, List<String> files, int i) {
			this.countDownLatch = countDownLatch;
			this.files = files;
			threadIndex = i;
		}

		public void run() {
			// TODO Auto-generated method stub
			if( !files.isEmpty()) {
				GoodHandler goodHandler = new GoodHandler( systemImpl, threadIndex, countDownLatch);
				goodHandler.HandleGoodFiles(files);
			}
			else {
				countDownLatch.countDown();
			}
			
		}
	}

	class OrderRun implements Runnable {
		CountDownLatch countDownLatch;
		List<String> files;
		int threadIndex= 0;

		public OrderRun(CountDownLatch countDownLatch, List<String> files, int i) {
			this.countDownLatch = countDownLatch;
			this.files = files;
			this.threadIndex = i;
		}

		public void run() {
			// TODO Auto-generated method stub
			if( !files.isEmpty()) {
				OrderHandler orderHandler = new OrderHandler(systemImpl, threadIndex, countDownLatch);
				orderHandler.HandleOrderFiles(files);
			}
			else {
				for( int i = 0; i< 3; i++) {
					countDownLatch.countDown();
				}
				
			}
			//countDownLatch.countDown();
		}
	}

	public ConstructSystem(OrderSystemImpl systemImpl) {
		// TODO Auto-generated constructor stub
		this.systemImpl = systemImpl;
	}

	/**
	 * 
	 * @param buyerfiles
	 * @param goodfiles
	 * @param orderfiles
	 * @param storeFolders
	 * @param threadNum
	 */
	public void startHandling(Collection<String> buyerfiles,
			Collection<String> goodfiles, Collection<String> orderfiles,
			Collection<String> storeFolders, int threadNum) {
		long startTime = System.currentTimeMillis();

		CountDownLatch countDownLatch;
		try {
			// 处理buyer表
			countDownLatch = new CountDownLatch(threadNum);
			for (int i = 0; i < threadNum; i++) {
				List<String> files = getGroupFiles(buyerfiles, i, threadNum);
				new Thread(new BuyerRun(countDownLatch, files, i )).start();
			}
			countDownLatch.await();
			System.out.println("buyer time:"
					+ (System.currentTimeMillis() - startTime) / 1000);

			// 处理good表
			countDownLatch = new CountDownLatch(threadNum);
			for (int i = 0; i < threadNum; i++) {
				List<String> files = getGroupFiles(goodfiles, i, threadNum);
				new Thread(new GoodRun(countDownLatch, files, i)).start();
			}
			countDownLatch.await();

			// 处理order表
			System.out.println("good time:"
					+ (System.currentTimeMillis() - startTime) / 1000);
			countDownLatch = new CountDownLatch(threadNum * 3);
			for (int i = 0; i < threadNum; i++) {
				List<String> files = getGroupFiles(orderfiles, i, threadNum);
				new Thread(new OrderRun(countDownLatch, files, i)).start();
			}
			countDownLatch.await();

			System.out.println("order time:"
					+ (System.currentTimeMillis() - startTime) / 1000);

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private List<String> getGroupFiles(Collection<String> files, int group,
			int readers) {
		// 分给多个读线程
		
		List<String> list = new ArrayList<String>();
		if( readers == 3) {
			for( String file: files) {
				if( file.startsWith(RaceConfig.storeFolders[group])) {
					list.add(file);
				}
			}
		}
		if( list.isEmpty() ) {
			List<String> fileList = new ArrayList<String>(files);
			for (int i = group; i < fileList.size(); i += readers) {
				list.add(fileList.get(i));
			}
		}
		return list;
	}

}
