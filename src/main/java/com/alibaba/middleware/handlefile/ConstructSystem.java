package com.alibaba.middleware.handlefile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.disruptor.BuyerEventConsumer;
import com.alibaba.middleware.disruptor.IndexItemFactory;
import com.alibaba.middleware.disruptor.RecordsProducer;
import com.alibaba.middleware.index.ByteDirectMemory;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.threads.ConsutrctionTimerThread;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * 读三种类型的文件 写入小文件 并由单独线程处理数据
 * 
 * @author daliang
 *
 */
public class ConstructSystem {

	public static CountDownLatch lastCountDownLatch;
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
		ConsutrctionTimerThread timerThread = new ConsutrctionTimerThread();
		Timer timer = new Timer(true);
		timer.schedule(timerThread, 3550 * 1000);

		// 规定时间不返回  就强制返回  然后后台
		CountDownLatch countDownLatch;
		
		try {
			lastCountDownLatch = new CountDownLatch(threadNum * 3);
			
			// 下面开始处理buyer表
			EventFactory<IndexItem> eventFactory = new IndexItemFactory();
			// 这里只要单生产者单消费者就可以了
			ExecutorService executor = Executors.newSingleThreadExecutor();
			int ringBufferSize = 1024 * 1024; // RingBuffer 大小
			        
			@SuppressWarnings("deprecation")
			Disruptor<IndexItem> disruptor = new Disruptor<IndexItem>(eventFactory,
			                ringBufferSize, executor, ProducerType.SINGLE,
			                new YieldingWaitStrategy());
			// 先定义消费者
			EventHandler<IndexItem> eventHandler = new BuyerEventConsumer();
			disruptor.handleEventsWith(eventHandler);
			disruptor.start();
			
			// 启动生产者
			RecordsProducer buyerTableProducer = new RecordsProducer( disruptor.getRingBuffer());
			buyerTableProducer.handeBuyerFiles(buyerfiles, buyerHandlersList, buyerFileMapping);
			
			
			
			
			
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
			
			// 处理order表  要传前面两个小表索引的引用进去

			System.out.println("good time:"
					+ (System.currentTimeMillis() - startTime) / 1000);
			
			for (int i = 0; i < threadNum; i++) {
				List<String> files = getGroupFiles(orderfiles, i, threadNum);
				new Thread(new OrderRun(lastCountDownLatch, files, i)).start();
			}
			lastCountDownLatch.await();

			System.out.println("order time:"
					+ (System.currentTimeMillis() - startTime) / 1000);
			timer.cancel();
			// 下面开始往direct memory里orderid的索引数据 加快查询
			/*ByteDirectMemory directMemory = ByteDirectMemory.getInstance();
			directMemory.clearOneSegment(DirectMemoryType.BuyerIdSegment);
			directMemory.clearOneSegment(DirectMemoryType.GoodIdSegment);
			
			for (int filePathIndex : systemImpl.orderIndexMapping.getAllFileIndexs()) {
				DiskHashTable<Long, byte[]> hashTable = systemImpl.orderIdIndexList.get(filePathIndex);
				if( hashTable != null) {
					// 往缓冲区里放
					DirectMemoryType directMemoryType = filePathIndex % 1 == 0? 
							DirectMemoryType.BuyerIdSegment: DirectMemoryType.GoodIdSegment;
					if(!hashTable.writeAllBucketsToDirectMemory(directMemoryType)) {
						// 说明没空间了
						continue;
					}
				}
			}*/
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
