package com.alibaba.middleware.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.tools.ByteUtils;

public class ByteDirectMemory {

	//private ByteBuffer orderIdBuffer;
	public ByteBuffer orderBuyerBuffer;
	public ByteBuffer orderGoodBuffer;
	//private ReentrantReadWriteLock mainSegLock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock orderBuyerSegLock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock orderGoodSegLock = new ReentrantReadWriteLock();
	private static ByteDirectMemory instance = null;
	
	// 用于判断队列是否满
	//private boolean mainSegIsFull = false;
	private boolean orderBuyerSegIsFull = false;
	private boolean orderGoodSegIsFull = false;
	
	public int orderBuyerPreserveSpace = 0;
	public int orderGoodPreserveSpace = 0;
	
	// 记录队列最后的位置
	//private int mainSegOffset = 0;
	private int orderBuyerSegOffset = 0;
	private int orderGoodSegOffset = 0;
	
	public static ByteDirectMemory getInstance() {
		if(instance == null) {
			instance = new ByteDirectMemory(RaceConfig.buyerDirectMemorySize, RaceConfig.goodDirectMemorySize);
		}
		return instance;
	}
	
	public ByteDirectMemory(int buyerSize, int goodSize) {
		// TODO Auto-generated constructor stub
		//orderIdBuffer = ByteBuffer.allocateDirect(size);
		orderBuyerPreserveSpace = RaceConfig.buyer_remaining_bytes_length;
		orderGoodPreserveSpace = RaceConfig.good_remaining_bytes_length;
		orderBuyerBuffer = ByteBuffer.allocateDirect(buyerSize);
		orderGoodBuffer = ByteBuffer.allocateDirect(goodSize);
	}

	public long getPosition( DirectMemoryType memoryType) {
		// TODO Auto-generated method stub
		long pos = 0;
		switch( memoryType) {
/*		case MainSegment:
			mainSegLock.readLock().lock();
			pos = mainSegOffset;
			mainSegLock.readLock().unlock();
			break;*/
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
	public void appendByteToPosAndUpdate(byte[] byteArray, int oldPos, int oldSize, 
			DirectMemoryType memoryType) {
		
		// TODO Auto-generated method stub
		switch( memoryType) {
/*		case MainSegment:
			mainSegLock.writeLock().lock();
			orderIdBuffer.position(mainSegOffset);
			if( orderIdBuffer.remaining() < byteArray.length) {
				// 说明空间不够了
				mainSegIsFull = true;
			}
			else {
				newPos = mainSegOffset;
				orderIdBuffer.putInt(byteArray.length);
				orderIdBuffer.put(byteArray);
				mainSegOffset = orderIdBuffer.position();
			}
			mainSegLock.writeLock().unlock();
			break;*/
		case BuyerIdSegment:
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.position(oldPos);
			orderBuyerBuffer.putInt(oldSize + byteArray.length);	// 更新大小
			orderBuyerBuffer.position(oldSize + oldPos + RaceConfig.int_size);			// 定位到旧数据的尾部
			orderBuyerBuffer.put(byteArray);						// 放入数据
			orderBuyerSegLock.writeLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position(oldPos);
			orderGoodBuffer.putInt(oldSize + byteArray.length);	// 更新大小
			orderGoodBuffer.position(oldSize + oldPos + RaceConfig.int_size);			// 定位到旧数据的尾部
			orderGoodBuffer.put(byteArray);						// 放入数据
			orderGoodSegLock.writeLock().unlock();
			break;
		}
	}
	
	/**
	 * 将字节数组放入直接内存中 直接内存分为三段  这里需要判断是否还有剩余空间
	 * 
	 * 返回写完之后的pos  如果pos为0则说明写入没有成功
	 * @param byteArray
	 * @param segment
	 */
	public void putInSprcificPos(byte[] byteArray, int pos, DirectMemoryType memoryType) {
		
		// TODO Auto-generated method stub
		switch( memoryType) {
/*		case MainSegment:
			mainSegLock.writeLock().lock();
			orderIdBuffer.position(mainSegOffset);
			if( orderIdBuffer.remaining() < byteArray.length) {
				// 说明空间不够了
				mainSegIsFull = true;
			}
			else {
				newPos = mainSegOffset;
				orderIdBuffer.putInt(byteArray.length);
				orderIdBuffer.put(byteArray);
				mainSegOffset = orderIdBuffer.position();
			}
			mainSegLock.writeLock().unlock();
			break;*/
		case BuyerIdSegment:
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.position(pos);
			orderBuyerBuffer.putInt(byteArray.length);
			orderBuyerBuffer.put(byteArray);
			orderBuyerSegLock.writeLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position(pos);
			orderGoodBuffer.putInt(byteArray.length);
			orderGoodBuffer.put(byteArray);
			orderGoodSegLock.writeLock().unlock();
			break;
		}
	}
	
	/**
	 * 将字节数组放入直接内存中 在末尾写入1024个空白字节 以便后来修改用
	 * 
	 * 返回写完之后的pos  如果pos为0则说明写入没有成功
	 * @param byteArray
	 * @param segment
	 */
	public int putAndAppendRemaining(byte[] byteArray, DirectMemoryType memoryType) {
		
		// TODO Auto-generated method stub
		int newPos = -1;
		switch( memoryType) {
/*		case MainSegment:
			mainSegLock.writeLock().lock();
			orderIdBuffer.position(mainSegOffset);
			if( orderIdBuffer.remaining() < byteArray.length) {
				// 说明空间不够了
				mainSegIsFull = true;
			}
			else {
				newPos = mainSegOffset;
				orderIdBuffer.putInt(byteArray.length);
				orderIdBuffer.put(byteArray);
				mainSegOffset = orderIdBuffer.position();
			}
			mainSegLock.writeLock().unlock();
			break;*/
		case BuyerIdSegment:
			orderBuyerSegLock.writeLock().lock();
			
			orderBuyerBuffer.position(orderBuyerSegOffset);
			if( orderBuyerBuffer.remaining() < byteArray.length + RaceConfig.int_size +
					orderBuyerPreserveSpace) {
				// 说明空间不够了 下次初始化时  每个offset的预留空间减半  
				System.out.println("orderBuyerBuffer direct memory have no space!");
				orderBuyerSegIsFull = true;
			}
			else {
				// 先写int代表大小
				newPos = orderBuyerSegOffset;
				orderBuyerBuffer.putInt(byteArray.length);
				orderBuyerBuffer.put(byteArray);
				orderBuyerBuffer.put(new byte[orderBuyerPreserveSpace]);
				orderBuyerSegOffset = orderBuyerBuffer.position();
			}
			orderBuyerSegLock.writeLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position(orderGoodSegOffset);
			if( orderGoodBuffer.remaining() < byteArray.length + RaceConfig.int_size +
					orderGoodPreserveSpace) {
				// 说明空间不够了
				System.out.println("orderGoodBuffer direct memory have no space!");
				orderGoodSegIsFull = true;
			}
			else {
				newPos = orderGoodSegOffset;
				orderGoodBuffer.putInt(byteArray.length);
				orderGoodBuffer.put(byteArray);
				orderGoodBuffer.put(new byte[orderGoodPreserveSpace]);
				orderGoodSegOffset = orderGoodBuffer.position();
			}
			
			orderGoodSegLock.writeLock().unlock();
			break;
		}
		return newPos;
	}

	/**
	 * 将字节数组放入直接内存中 直接内存分为三段  这里需要判断是否还有剩余空间
	 * 
	 * 返回写完之后的pos  如果pos为0则说明写入没有成功
	 * @param byteArray
	 * @param segment
	 */
	public int put(byte[] byteArray, DirectMemoryType memoryType) {
		
		// TODO Auto-generated method stub
		int newPos = -1;
		switch( memoryType) {
/*		case MainSegment:
			mainSegLock.writeLock().lock();
			orderIdBuffer.position(mainSegOffset);
			if( orderIdBuffer.remaining() < byteArray.length) {
				// 说明空间不够了
				mainSegIsFull = true;
			}
			else {
				newPos = mainSegOffset;
				orderIdBuffer.putInt(byteArray.length);
				orderIdBuffer.put(byteArray);
				mainSegOffset = orderIdBuffer.position();
			}
			mainSegLock.writeLock().unlock();
			break;*/
		case BuyerIdSegment:
			orderBuyerSegLock.writeLock().lock();
			
			orderBuyerBuffer.position(orderBuyerSegOffset);
			if( orderBuyerBuffer.remaining() < byteArray.length) {
				// 说明空间不够了
				orderBuyerSegIsFull = true;
			}
			else {
				// 先写int代表大小
				newPos = orderBuyerSegOffset;
				orderBuyerBuffer.putInt(byteArray.length);
				orderBuyerBuffer.put(byteArray);
				orderBuyerSegOffset = orderBuyerBuffer.position();
			}
			orderBuyerSegLock.writeLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position(orderGoodSegOffset);
			if( orderGoodBuffer.remaining() < byteArray.length) {
				// 说明空间不够了
				orderGoodSegIsFull = true;
			}
			else {
				newPos = orderGoodSegOffset;
				orderGoodBuffer.putInt(byteArray.length);
				orderGoodBuffer.put(byteArray);
				orderGoodSegOffset = orderGoodBuffer.position();
			}
			
			orderGoodSegLock.writeLock().unlock();
			break;
		}
		return newPos;
	}

	/**
	 * 得到相应位置后面的byte大小的int
	 * @param position
	 * @param memoryType
	 * @return
	 */
	public int getByteSize(int position,DirectMemoryType memoryType ) {
		// TODO Auto-generated method stub
		int content = -1;
		switch( memoryType) {
/*		case MainSegment:
			mainSegLock.writeLock().lock();
			orderIdBuffer.position( position);
			content = new byte[orderIdBuffer.getInt()];
			orderIdBuffer.get(content);
			mainSegLock.writeLock().unlock();
			break;*/
		case BuyerIdSegment:
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.position(position);
			content = orderBuyerBuffer.getInt();
			orderBuyerSegLock.writeLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position( position);
			content = orderGoodBuffer.getInt();
			orderGoodSegLock.writeLock().unlock();
			break;
		}
		return content;
	}
	
	public List<byte[]> getOrderIdListsFromBytes(int position,DirectMemoryType memoryType ) {
		// TODO Auto-generated method stub
		List<byte[]> content = null;
		switch( memoryType) {
/*		case MainSegment:
			mainSegLock.writeLock().lock();
			orderIdBuffer.position( position);
			content = new byte[orderIdBuffer.getInt()];
			orderIdBuffer.get(content);
			mainSegLock.writeLock().unlock();
			break;*/
		case BuyerIdSegment:
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.position(position);
			byte[] bytes = new byte[orderBuyerBuffer.getInt()];
			orderBuyerBuffer.get(bytes);
			content = ByteUtils.splitBytes(bytes);
			orderBuyerSegLock.writeLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position(position);
			byte[] goodBytes = new byte[orderGoodBuffer.getInt()];
			orderGoodBuffer.get(goodBytes);
			content = ByteUtils.splitBytes(goodBytes);
			orderGoodSegLock.writeLock().unlock();
			break;
		}
		return content;
	}
	
	public byte[] get(int position,DirectMemoryType memoryType ) {
		// TODO Auto-generated method stub
		byte[] content = null;
		switch( memoryType) {
/*		case MainSegment:
			mainSegLock.writeLock().lock();
			orderIdBuffer.position( position);
			content = new byte[orderIdBuffer.getInt()];
			orderIdBuffer.get(content);
			mainSegLock.writeLock().unlock();
			break;*/
		case BuyerIdSegment:
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.position(position);
			content = new byte[orderBuyerBuffer.getInt()];
			
			orderBuyerBuffer.get(content);
			orderBuyerSegLock.writeLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position( position);
			int a = orderGoodBuffer.getInt();
			if( a < 0) {
				System.out.println("negative");
			}
			content = new byte[a];
			orderGoodBuffer.get(content);
			orderGoodSegLock.writeLock().unlock();
			break;
		}
		return content;
	}
	
	public boolean isFull(DirectMemoryType memoryType ) {
		switch( memoryType) {
/*		case MainSegment:
			return mainSegIsFull;*/
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
	/*public int getIntValue( int pos, DirectMemoryType memoryType) {
		switch( memoryType) {
		case MainSegment:
			return orderIdBuffer.getInt(pos);
		case BuyerIdSegment:
			return orderBuyerBuffer.getInt(pos);
		case GoodIdSegment:
			return orderGoodBuffer.getInt(pos);
		}
		return 0;
	}*/
	
	public void clear(){
		//orderIdBuffer.clear();
		System.out.println("buyer direct pos:" + orderBuyerSegOffset);
		System.out.println("good direct pos:" + orderGoodSegOffset);
		orderGoodBuffer.clear();
		orderBuyerBuffer.clear();
	}
	
	public void clearOneSegment(DirectMemoryType memoryType) {
		switch( memoryType) {
		case BuyerIdSegment:
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.clear();
			orderBuyerSegIsFull = false;
			orderBuyerSegOffset = 0;
			orderBuyerSegLock.writeLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.clear();
			orderGoodSegIsFull = false;
			orderGoodSegOffset = 0;
			orderGoodSegLock.writeLock().unlock();
			break;
			
		}
	}
}
