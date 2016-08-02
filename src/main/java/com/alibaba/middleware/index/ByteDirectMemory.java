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
	public ByteBuffer sharedDirectBuffer;				// 由于每块最多2GB  这里是共享的direct memory
	//private ReentrantReadWriteLock mainSegLock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock orderBuyerSegLock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock orderGoodSegLock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock sharedSegLock = new ReentrantReadWriteLock();
	private static ByteDirectMemory instance = null;
	
	// 用于判断队列是否满
	//private boolean mainSegIsFull = false;
	private boolean orderBuyerSegIsFull = false;
	private boolean orderGoodSegIsFull = false;
	private boolean sharedSegIsFull = false;
	
	public int orderBuyerPreserveSpace = 0;
	public int orderGoodPreserveSpace = 0;
	
	// 记录队列最后的位置
	//private int mainSegOffset = 0;
	private int orderBuyerSegOffset = 0;
	private int orderGoodSegOffset = 0;
	private int sharedSegOffset = 0;
	
	public static ByteDirectMemory getInstance() {
		if(instance == null) {
			instance = new ByteDirectMemory(RaceConfig.directMemorySize);
		}
		return instance;
	}
	
	public ByteDirectMemory(int size) {
		// TODO Auto-generated constructor stub
		//orderIdBuffer = ByteBuffer.allocateDirect(size);
		orderBuyerPreserveSpace = RaceConfig.buyer_remaining_bytes_length;
		orderGoodPreserveSpace = RaceConfig.good_remaining_bytes_length;
		orderBuyerBuffer = ByteBuffer.allocateDirect(size);
		orderGoodBuffer = ByteBuffer.allocateDirect(size);
		sharedDirectBuffer = ByteBuffer.allocateDirect(size);
	}

//	public long getPosition( DirectMemoryType memoryType) {
//		// TODO Auto-generated method stub
//		long pos = 0;
//		switch( memoryType) {
///*		case MainSegment:
//			mainSegLock.readLock().lock();
//			pos = mainSegOffset;
//			mainSegLock.readLock().unlock();
//			break;*/
//		case BuyerIdSegment:
//			orderBuyerSegLock.readLock().lock();
//			pos= orderBuyerSegOffset;
//			orderBuyerSegLock.readLock().unlock();
//			break;
//		case GoodIdSegment:
//			orderGoodSegLock.readLock().lock();
//			pos= orderGoodSegOffset;
//			orderGoodSegLock.readLock().unlock();
//			break;
//		}
//		return pos;
//		
//	}
	
	/**
	 * 将字节数组放入直接内存中 直接内存分为三段  这里需要判断是否还有剩余空间
	 * 
	 * 返回写完之后的pos  如果pos为0则说明写入没有成功
	 * @param byteArray
	 * @param segment
	 */
	public void appendByteToPosAndUpdate(int memoryType, byte[] byteArray, int oldPos, int oldSize) {
		// TODO Auto-generated method stub
		
		if( memoryType == RaceConfig.buyerMemory) {
			// 从buyer缓冲区里拿
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.position(oldPos);
			orderBuyerBuffer.putInt(oldSize + byteArray.length);	// 更新大小
			orderBuyerBuffer.position(oldSize + oldPos + RaceConfig.int_size);			// 定位到旧数据的尾部
			orderBuyerBuffer.put(byteArray);						// 放入数据
			orderBuyerSegLock.writeLock().unlock();
		}
		else if( memoryType == RaceConfig.goodMemory){
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position(oldPos);
			orderGoodBuffer.putInt(oldSize + byteArray.length);	// 更新大小
			orderGoodBuffer.position(oldSize + oldPos + RaceConfig.int_size);			// 定位到旧数据的尾部
			orderGoodBuffer.put(byteArray);						// 放入数据
			orderGoodSegLock.writeLock().unlock();
		}
		else {
			sharedSegLock.writeLock().lock();
			sharedDirectBuffer.position(oldPos);
			sharedDirectBuffer.putInt(oldSize + byteArray.length);	// 更新大小
			sharedDirectBuffer.position(oldSize + oldPos + RaceConfig.int_size);			// 定位到旧数据的尾部
			sharedDirectBuffer.put(byteArray);						// 放入数据
			sharedSegLock.writeLock().unlock();
			
		}

	}
	
	/**
	 * 将字节数组放入直接内存中 直接内存分为三段  这里需要判断是否还有剩余空间
	 * 
	 * 返回写完之后的pos  如果pos为0则说明写入没有成功
	 * @param byteArray
	 * @param segment
	 */
	public void putInSprcificPos(int memoryType ,byte[] byteArray, int pos) {
		
		// TODO Auto-generated method stub
		
		if( memoryType == RaceConfig.buyerMemory) {
			// 从buyer缓冲区里拿
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.position(pos);
			orderBuyerBuffer.putInt(byteArray.length);
			orderBuyerBuffer.put(byteArray);
			orderBuyerSegLock.writeLock().unlock();
		}
		else if( memoryType == RaceConfig.goodMemory){
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position(pos);
			orderGoodBuffer.putInt(byteArray.length);
			orderGoodBuffer.put(byteArray);
			orderGoodSegLock.writeLock().unlock();
		}
		else {
			sharedSegLock.writeLock().lock();
			sharedDirectBuffer.position(pos);
			sharedDirectBuffer.putInt(byteArray.length);
			sharedDirectBuffer.put(byteArray);
			sharedSegLock.writeLock().unlock();
		}
	}
	
	/**
	 * 将字节数组放入直接内存中 在末尾写入1024个空白字节 以便后来修改用
	 * 
	 * 
	 * 返回int数组  第一个代表写到哪个缓冲区了  第二个代表实际的位置信息
	 * @param byteArray
	 * @param segment
	 */
	public int[] putAndAppendRemaining(byte[] byteArray, DirectMemoryType memoryType, 
			int reserveSize) {
		
		// TODO Auto-generated method stub
		int[] resultInfo = new int[2];
		switch( memoryType) {
		case BuyerIdSegment:
			orderBuyerSegLock.writeLock().lock();
			
			orderBuyerBuffer.position(orderBuyerSegOffset);
			if( orderBuyerSegIsFull || orderBuyerBuffer.remaining() <
					byteArray.length + RaceConfig.int_size +
					reserveSize) {
				// 说明空间不够了 写到shared memory里去
				
				//System.out.println("orderBuyerBuffer direct memory have no space");
				orderBuyerSegIsFull = true;
				resultInfo[0] = RaceConfig.sharedMemory;
				resultInfo[1] = putToSharedMemory(byteArray, reserveSize);
			}
			else {
				// 先写int代表大小
				resultInfo[0] = RaceConfig.buyerMemory;					// 0代表
				resultInfo[1] = orderBuyerSegOffset;
				orderBuyerBuffer.putInt(byteArray.length);
				orderBuyerBuffer.put(byteArray);
				orderBuyerBuffer.put(new byte[reserveSize]);
				orderBuyerSegOffset = orderBuyerBuffer.position();
			}
			orderBuyerSegLock.writeLock().unlock();
			break;
		case GoodIdSegment:
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position(orderGoodSegOffset);
			if( orderGoodSegIsFull || orderGoodBuffer.remaining() <
					byteArray.length + RaceConfig.int_size +
					reserveSize) {
				// 说明空间不够了
				//System.out.println("orderGoodBuffer direct memory have no space!");
				orderGoodSegIsFull = true;
				resultInfo[0] = RaceConfig.sharedMemory;
				resultInfo[1] = putToSharedMemory(byteArray, reserveSize);
			}
			else {
				resultInfo[0] = RaceConfig.goodMemory;
				resultInfo[1] = orderGoodSegOffset;
				orderGoodBuffer.putInt(byteArray.length);
				orderGoodBuffer.put(byteArray);
				orderGoodBuffer.put(new byte[reserveSize]);
				orderGoodSegOffset = orderGoodBuffer.position();
			}
			
			orderGoodSegLock.writeLock().unlock();
			break;
		}
		return resultInfo;
	}
	
	/**
	 * 这里要确保只有一个方法进入
	 * @param byteArray
	 * @param reserveSize
	 * @return
	 */
	public synchronized int putToSharedMemory( byte[] byteArray, int reserveSize) {
		sharedSegLock.writeLock().lock();
		int pos = -1;
		sharedDirectBuffer.position(sharedSegOffset);
		if( sharedSegIsFull || sharedDirectBuffer.remaining() <
				byteArray.length + RaceConfig.int_size +
				reserveSize) {
			// 说明空间不够了
			
			System.out.println("all direct memory have no space");
			sharedSegIsFull = true;
		}
		else {
			// 先写int代表大小
			pos = sharedSegOffset;
			sharedDirectBuffer.putInt(byteArray.length);
			sharedDirectBuffer.put(byteArray);
			sharedDirectBuffer.put(new byte[reserveSize]);
			sharedSegOffset = sharedDirectBuffer.position();
		}
		sharedSegLock.writeLock().unlock();
		return pos;
		
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
	public int getByteSize(int memoryType ,int position) {
		// TODO Auto-generated method stub
		int content = -1;
		if( memoryType == RaceConfig.buyerMemory) {
			// 从buyer缓冲区里拿
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.position(position);
			content = orderBuyerBuffer.getInt();
			orderBuyerSegLock.writeLock().unlock();
		}
		else if( memoryType == RaceConfig.goodMemory){
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position( position);
			content = orderGoodBuffer.getInt();
			orderGoodSegLock.writeLock().unlock();
		}
		else {
			sharedSegLock.writeLock().lock();
			sharedDirectBuffer.position( position);
			content = sharedDirectBuffer.getInt();
			sharedSegLock.writeLock().unlock();
		}
		return content;
	}
	
	/**
	 * 查询用的
	 * @param directMemType
	 * @param position
	 * @return
	 */
	public List<byte[]> getOrderIdListsFromBytes(int directMemType, int position ) {
		// TODO Auto-generated method stub
		List<byte[]> content = null;
		if( directMemType == RaceConfig.buyerMemory) {
			// 从buyer缓冲区里拿
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.position(position);
			byte[] bytes = new byte[orderBuyerBuffer.getInt()];
			orderBuyerBuffer.get(bytes);
			content = ByteUtils.splitBytes(bytes);
			orderBuyerSegLock.writeLock().unlock();
		}
		else if( directMemType == RaceConfig.goodMemory){
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position(position);
			byte[] goodBytes = new byte[orderGoodBuffer.getInt()];
			orderGoodBuffer.get(goodBytes);
			content = ByteUtils.splitBytes(goodBytes);
			orderGoodSegLock.writeLock().unlock();
		}
		else {
			sharedSegLock.writeLock().lock();
			sharedDirectBuffer.position(position);
			byte[] bytes = new byte[sharedDirectBuffer.getInt()];
			sharedDirectBuffer.get(bytes);
			content = ByteUtils.splitBytes(bytes);
			sharedSegLock.writeLock().unlock();
		}
		return content;
	}
	
	/**
	 * 构建用的
	 * @param directMemType
	 * @param position
	 * @return
	 */
	public byte[] get(int directMemType, int position ) {
		// TODO Auto-generated method stub
		byte[] content = null;
		if( directMemType == RaceConfig.buyerMemory) {
			// 从buyer缓冲区里拿
			orderBuyerSegLock.writeLock().lock();
			orderBuyerBuffer.position(position);
			content = new byte[orderBuyerBuffer.getInt()];
			
			orderBuyerBuffer.get(content);
			orderBuyerSegLock.writeLock().unlock();
		}
		else if( directMemType == RaceConfig.goodMemory){
			orderGoodSegLock.writeLock().lock();
			orderGoodBuffer.position( position);
			content = new byte[orderGoodBuffer.getInt()];
			orderGoodBuffer.get(content);
			orderGoodSegLock.writeLock().unlock();
		}
		else {
			// 从shared memory里拿
			sharedSegLock.writeLock().lock();
			sharedDirectBuffer.position( position);
			content = new byte[sharedDirectBuffer.getInt()];
			sharedDirectBuffer.get(content);
			sharedSegLock.writeLock().unlock();
		}
		return content;
	}
	
//	public boolean isFull(DirectMemoryType memoryType ) {
//		switch( memoryType) {
///*		case MainSegment:
//			return mainSegIsFull;*/
//		case BuyerIdSegment:
//			return orderBuyerSegIsFull;
//		case GoodIdSegment:
//			return orderGoodSegIsFull;
//		}
//		return false;
//	}
	
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
		sharedDirectBuffer.clear();
	}
	
//	public void clearOneSegment(DirectMemoryType memoryType) {
//		switch( memoryType) {
//		case BuyerIdSegment:
//			orderBuyerSegLock.writeLock().lock();
//			orderBuyerBuffer.clear();
//			orderBuyerSegIsFull = false;
//			orderBuyerSegOffset = 0;
//			orderBuyerSegLock.writeLock().unlock();
//			break;
//		case GoodIdSegment:
//			orderGoodSegLock.writeLock().lock();
//			orderGoodBuffer.clear();
//			orderGoodSegIsFull = false;
//			orderGoodSegOffset = 0;
//			orderGoodSegLock.writeLock().unlock();
//			break;
//			
//		}
//	}
}
