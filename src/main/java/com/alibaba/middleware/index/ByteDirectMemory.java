package com.alibaba.middleware.index;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;

public class ByteDirectMemory {
	private ByteBuffer orderIdBuffer;
	private ByteBuffer orderBuyerBuffer;
	private ByteBuffer orderGoodBuffer;
	private ReentrantReadWriteLock mainSegLock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock orderBuyerSegLock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock orderGoodSegLock = new ReentrantReadWriteLock();
	private static ByteDirectMemory instance = null;
	
	public static ByteDirectMemory getInstance() {
		if(instance == null) {
			instance = new ByteDirectMemory(RaceConfig.directMemorySize);
		}
		return instance;
	}
	
	public ByteDirectMemory(int size) {
		// TODO Auto-generated constructor stub
		orderIdBuffer = ByteBuffer.allocateDirect(size);
		orderBuyerBuffer = ByteBuffer.allocateDirect(size);
		orderGoodBuffer = ByteBuffer.allocateDirect(size);
	}

	public long getPosition( DirectMemoryType memoryType) {
		// TODO Auto-generated method stub
		long pos = 0;
		switch( memoryType) {
		case MainSegment:
			mainSegLock.readLock().lock();
			pos =  orderIdBuffer.position();
			mainSegLock.readLock().unlock();
			break;
		case BuyerIdSegment:
			orderBuyerSegLock.readLock().lock();
			pos= orderBuyerBuffer.position();
			orderBuyerSegLock.readLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.readLock().lock();
			pos= orderGoodBuffer.position();
			orderGoodSegLock.readLock().unlock();
			break;
		}
		return pos;
		
	}

	/**
	 * 将字节数组放入直接内存中 直接内存分为三段
	 * @param byteArray
	 * @param segment
	 */
	public void put(byte[] byteArray, DirectMemoryType memoryType) {
		// TODO Auto-generated method stub
		switch( memoryType) {
		case MainSegment:
			mainSegLock.writeLock().lock();
			orderIdBuffer.put(byteArray);
			mainSegLock.writeLock().unlock();
			break;
		case BuyerIdSegment:
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.put(byteArray);
			orderBuyerSegLock.writeLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.put(byteArray);
			orderGoodSegLock.writeLock().unlock();
			break;
		}
		
	}

	public byte[] get(int position, int size,DirectMemoryType memoryType ) {
		// TODO Auto-generated method stub
		byte[] content = new byte[size];
		switch( memoryType) {
		case MainSegment:
			mainSegLock.writeLock().lock();
			orderIdBuffer.position( position);
			orderIdBuffer.get(content);
			mainSegLock.writeLock().unlock();
			break;
		case BuyerIdSegment:
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.position(position);
			orderBuyerBuffer.get(content);
			orderBuyerSegLock.writeLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position( position);
			orderGoodBuffer.get(content);
			orderGoodSegLock.writeLock().unlock();
			break;
		}
		return content;
	}
	
	public void clear(){
		orderIdBuffer.clear();
		orderGoodBuffer.clear();
		orderBuyerBuffer.clear();
	}
}
