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
	
	// 用于判断队列是否满
	private boolean mainSegIsFull = false;
	private boolean orderBuyerSegIsFull = false;
	private boolean orderGoodSegIsFull = false;
	
	// 记录队列最后的位置
	private int mainSegOffset = 0;
	private int orderBuyerSegOffset = 0;
	private int orderGoodSegOffset = 0;
	
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
			pos = mainSegOffset;
			mainSegLock.readLock().unlock();
			break;
		case BuyerIdSegment:
			orderBuyerSegLock.readLock().lock();
			pos= orderBuyerSegOffset;
			orderBuyerSegLock.readLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.readLock().lock();
			pos= orderGoodSegOffset;
			orderGoodSegLock.readLock().unlock();
			break;
		}
		return pos;
		
	}


	/**
	 * 将字节数组放入直接内存中 直接内存分为三段  这里需要判断是否还有剩余空间
	 * 
	 * 返回写完之后的pos  如果pos为0则说明写入没有成功
	 * @param byteArray
	 * @param segment
	 */
	public int put(byte[] byteArray, DirectMemoryType memoryType, boolean isBuilding) {
		
		// TODO Auto-generated method stub
		int newPos = -1;
		switch( memoryType) {
		case MainSegment:
			mainSegLock.writeLock().lock();
			newPos = mainSegOffset;
			orderIdBuffer.position(mainSegOffset);
			if( orderIdBuffer.remaining() < byteArray.length) {
				// 说明空间不够了
				mainSegIsFull = true;
			}
			else {
				if( !isBuilding) {
					// 这里主要是我不想改之前的逻辑了  但是查询阶段的时候  一定要写length在前面
					orderIdBuffer.putInt(byteArray.length);
				}
				orderIdBuffer.put(byteArray);
				mainSegOffset = orderIdBuffer.position();
			}
			mainSegLock.writeLock().unlock();
			break;
		case BuyerIdSegment:
			orderBuyerSegLock.writeLock().lock();
			newPos = orderBuyerSegOffset;
			orderBuyerBuffer.position(orderBuyerSegOffset);
			if( orderBuyerBuffer.remaining() < byteArray.length) {
				// 说明空间不够了
				orderBuyerSegIsFull = true;
			}
			else {
				if( !isBuilding) {
					// 这里主要是我不想改之前的逻辑了  但是查询阶段的时候  一定要写length在前面
					orderIdBuffer.putInt(byteArray.length);
				}
				orderBuyerBuffer.put(byteArray);
				orderBuyerSegOffset = orderBuyerBuffer.position();
			}
			orderBuyerSegLock.writeLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.writeLock().lock();
			newPos = orderGoodSegOffset;
			orderGoodBuffer.position(orderGoodSegOffset);
			if( orderGoodBuffer.remaining() < byteArray.length) {
				// 说明空间不够了
				orderGoodSegIsFull = true;
			}
			else {
				if( !isBuilding) {
					// 这里主要是我不想改之前的逻辑了  但是查询阶段的时候  一定要写length在前面
					orderIdBuffer.putInt(byteArray.length);
				}
				orderGoodBuffer.put(byteArray);
				orderGoodSegOffset = orderGoodBuffer.position();
			}
			
			orderGoodSegLock.writeLock().unlock();
			break;
		}
		return newPos;
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
	
	public boolean isFull(DirectMemoryType memoryType ) {
		switch( memoryType) {
		case MainSegment:
			return mainSegIsFull;
		case BuyerIdSegment:
			return orderBuyerSegIsFull;
		case GoodIdSegment:
			return orderGoodSegIsFull;
		}
		return false;
	}
	
	/**
	 * 从pos位置读取一个int  这个int代表这个对象的大小 用于创建byte数组进而调用get方法
	 * @param pos
	 * @param memoryType
	 * @return
	 */
	public int getIntValue( int pos, DirectMemoryType memoryType) {
		switch( memoryType) {
		case MainSegment:
			return orderIdBuffer.getInt(pos);
		case BuyerIdSegment:
			return orderBuyerBuffer.getInt(pos);
		case GoodIdSegment:
			return orderGoodBuffer.getInt(pos);
		}
		return 0;
	}
	
	public void clear(){
		orderIdBuffer.clear();
		orderGoodBuffer.clear();
		orderBuyerBuffer.clear();
	}
}
