package com.alibaba.middleware.handlefile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.tools.FilePathWithIndex;

/**
 * 读三种类型的文件 写入小文件 并由单独线程处理数据
 * 
 * @author daliang
 *
 */
public class ConstructSystem {

	// 代理映射表
	// AgentMapping agentBuyerMapping;
	// AgentMapping agentGoodMapping;
	// 存订单表里的orderId索引<文件名（尽量短名）,内存里缓存的索引DiskHashTable>
	public ConcurrentHashMap<String, DiskHashTable<Long, Long>> orderIdIndexList = null;
	// 订单表里的buyerId代理键索引
	public ConcurrentHashMap<String, DiskHashTable<Long, List<Long>>> orderBuyerIdIndexList = null;
	// 订单表里的goodId代理键索引
	public ConcurrentHashMap<String, DiskHashTable<Long, List<Long>>> orderGoodIdIndexList = null;
	// 订单表里的可计算字段索引Map
	public ConcurrentHashMap<String, List<DiskHashTable<Long, List<Long>>>> orderCountableIndexList = null;
	// buyerId里的buyerId代理键索引
	public ConcurrentHashMap<String, DiskHashTable<Long, Long>> buyerIdIndexList = null;
	// goodId里的goodId代理键索引
	public ConcurrentHashMap<String, DiskHashTable<Long, Long>> goodIdIndexList = null;

	public List<FilePathWithIndex> orderFileList = null; // 保存order表所有文件的名字
	public List<FilePathWithIndex> buyerFileList = null; // 保存buyer表所有文件的名字
	public List<FilePathWithIndex> goodFileList = null; // 保存good表所有文件的名字

	public HashSet<String> orderAttrList = null; // 保存order表的所有字段名称
	public HashSet<String> buyerAttrList = null; // 保存buyer表的所有字段名称
	public HashSet<String> goodAttrList = null; // 保存good表的所有字段名称

	public FilePathWithIndex buyerIdSurrKeyFile = null; // 存代理键索引块的文件地址和索引元数据偏移地址
	public FilePathWithIndex goodIdSurrKeyFile = null; // 存代理键索引块的文件地址和索引元数据偏移地址
	public DiskHashTable<String, Long> buyerIdSurrKeyIndex = null; // 缓存buyerId事实键与代理键
	public DiskHashTable<String, Long> goodIdSurrKeyIndex = null; // 缓存goodId事实键与代理键
	
	HashMap<String, Boolean> computableItems;

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
			
			BuyerHandler buyerHandler = new BuyerHandler( buyerFileList, buyerAttrList,
					buyerIdSurrKeyFile, buyerIdIndexList,buyerIdSurrKeyIndex, threadIndex);
			buyerHandler.handeBuyerFiles(files);
			countDownLatch.countDown();
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
			GoodHandler goodHandler = new GoodHandler( goodFileList, goodAttrList,
					goodIdSurrKeyFile, goodIdIndexList,goodIdSurrKeyIndex, threadIndex);
			goodHandler.HandleGoodFiles(files);
			countDownLatch.countDown();
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
			OrderHandler orderHandler = new OrderHandler(orderIdIndexList, orderBuyerIdIndexList,
					orderGoodIdIndexList, orderCountableIndexList, orderFileList, orderAttrList,
					buyerIdSurrKeyIndex, goodIdSurrKeyIndex,threadIndex);
			orderHandler.HandleOrderFiles(files);
			countDownLatch.countDown();
		}
	}

	public ConstructSystem(ConcurrentHashMap<String, DiskHashTable<Long, Long>> orderIdIndexList,
			ConcurrentHashMap<String, DiskHashTable<Long, List<Long>>> orderBuyerIdIndexList, 
			ConcurrentHashMap<String, DiskHashTable<Long, List<Long>>> orderGoodIdIndexList, 
			ConcurrentHashMap<String, List<DiskHashTable<Long, List<Long>>>> orderCountableIndexList, 
			List<FilePathWithIndex> orderFileList, List<FilePathWithIndex> buyerFileList, 
			List<FilePathWithIndex> goodFileList, HashSet<String> orderAttrList, 
			HashSet<String> buyerAttrList, HashSet<String> goodAttrList, 
			FilePathWithIndex buyerIdSurrKeyFile, FilePathWithIndex goodIdSurrKeyFile, 
			ConcurrentHashMap<String, DiskHashTable<Long, Long>> buyerIdIndexList, 
			ConcurrentHashMap<String, DiskHashTable<Long, Long>> goodIdIndexList, 
			DiskHashTable<String, Long> buyerIdSurrKeyIndex,
			DiskHashTable<String, Long> goodIdSurrKeyIndex) {
		// TODO Auto-generated constructor stub
		this.orderIdIndexList = orderIdIndexList;
		this.buyerIdIndexList = buyerIdIndexList;
		this.orderBuyerIdIndexList = orderBuyerIdIndexList;
		this.orderGoodIdIndexList = orderGoodIdIndexList;
		this.orderCountableIndexList = orderCountableIndexList;
		this.orderFileList = orderFileList;
		this.buyerFileList = buyerFileList;
		this.goodFileList = goodFileList;
		this.orderAttrList = orderAttrList;
		this.buyerAttrList = buyerAttrList;
		this.goodAttrList = goodAttrList;
		this.buyerIdSurrKeyFile = buyerIdSurrKeyFile;
		this.goodIdSurrKeyFile = goodIdSurrKeyFile;
		this.goodIdIndexList = goodIdIndexList;
		this.buyerIdSurrKeyIndex = buyerIdSurrKeyIndex;
		this.goodIdSurrKeyIndex = goodIdSurrKeyIndex;
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
			countDownLatch = new CountDownLatch(threadNum);
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
		List<String> fileList = new ArrayList<String>(files);
		List<String> list = new ArrayList<String>();
		for (int i = group; i < fileList.size(); i += readers) {
			list.add(fileList.get(i));
		}
		return list;
	}

	// public static void main(String args[]){

	/*
	 * long startTime = System.currentTimeMillis(); List<String> buyerfiles =
	 * new ArrayList<String>(); buyerfiles.add("benchmark\\buyer_records.txt");
	 * buyerfiles.add("buyer_records_1.txt");
	 * buyerfiles.add("buyer_records_2.txt");
	 * 
	 * List<String> goodfiles = new ArrayList<String>();
	 * goodfiles.add("benchmark\\good_records.txt");
	 * goodfiles.add("good_records_1.txt"); goodfiles.add("good_records_2.txt");
	 * List<String> orderfiles = new ArrayList<String>();
	 * //orderfiles.add("order_records.txt");
	 * 
	 * ConstructSystem constructSystem = new ConstructSystem();
	 * constructSystem.startHandling(buyerfiles, goodfiles, orderfiles, 1);
	 * 
	 * System.out.println("order table time:" + (System.currentTimeMillis() -
	 * startTime) / 1000);
	 */
	// }
}
