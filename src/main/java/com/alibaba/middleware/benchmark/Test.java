package com.alibaba.middleware.benchmark;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.instrument.Instrumentation;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.middleware.cache.ConcurrentCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.index.ComparableKeys;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.RecordsUtils;

/**
 * 不用管
 * 
 * @author hankwing
 *
 */
public class Test {

	public static void main(String[] args) {
		
		/*ByteBuffer buffer = ByteBuffer.allocate(12);
		buffer.putInt(0);
		buffer.putLong(201238912);
		
		ByteBuffer decodedBuffer = ByteBuffer.wrap(buffer.array());
		System.out.println("int:" + decodedBuffer.getInt()+ " long:" + decodedBuffer.getLong());
		
		ConcurrentCache cache = ConcurrentCache.getInstance();
		
		cache.putInCache(buffer.array(), "hehe:hehe", TableName.OrderTable);
		
		ByteBuffer buffer2 = ByteBuffer.allocate(12);
		buffer2.putInt(0);
		buffer2.putLong(201238912);
		
		Row row = cache.getFromCache(buffer2.array(), TableName.OrderTable);
		
		System.out.println(row);*/
		
		
		List<Map<Integer, byte[]>> bucketAddressList = new ArrayList<Map<Integer, byte[]>>(); // 桶对应的物理地址
		for(int i = 0; i< 500; i++) {
			ConcurrentHashMap<Integer,byte[]>  bucketList= new ConcurrentHashMap<Integer, byte[]>();
			for( int j = 0; j < 1000000; j++) {
				ByteBuffer longValue = ByteBuffer.allocate(12);
				longValue.putInt(j);
				longValue.putLong(new Long(j));
				bucketList.put(i, longValue.array());
			}
			bucketAddressList.add(bucketList);
			
		}
		
		int a = 0;
		
		/*List<RandomAccessFile> files = new ArrayList<RandomAccessFile>();
		 String line = null;
		 long offset = 0;
		 long mStartTime = System.currentTimeMillis();
		 DiskHashTable<Integer, List<Long>> buyerIdHashTable = new DiskHashTable<Integer,List<Long>>(
					"temp" + RaceConfig.buyerIndexFileSuffix ,"temp", Long.class);
		 DiskHashTable<Integer, List<Long>> dHashTable = new DiskHashTable<Integer,List<Long>>(
					"temp" + RaceConfig.buyerIndexFileSuffix ,"temp", Long.class);
		 HashMap<Integer,Long> map = new HashMap<Integer,Long>();
		 HashMap<Integer,Long> map2 = new HashMap<Integer,Long>();
		 HashMap<Integer,Long> map3 = new HashMap<Integer,Long>();
		try {
			
			
			offset = 0;
			while( offset < 10000) {
				RandomAccessFile reader = new RandomAccessFile("benchmark/order_records.txt","r");
				files.add(reader);
				offset ++;
				long id = 0;
				
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long mEndTime = System.currentTimeMillis();
		//long offset2 = map.get(0);
		System.out.println("test2:" + (mEndTime - mStartTime) );*/
		
		
		
		
		
		/* long startTime = System.currentTimeMillis();
		 RandomAccessFile file;
		try {
			file = new RandomAccessFile("folder1/buyer_0","r");
			offset = 0;
			 line = RecordsUtils.getStringFromFile(file, offset);
			 while( line != null ) {
				 offset = 8000000;
				 line = RecordsUtils.getStringFromFile(file, offset);
			 }
			 long endTime = System.currentTimeMillis();
			 System.out.println("test1:" + (endTime - startTime));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		/*
		 * TreeMap<String, HashMap<String, Long>> treeMap = new TreeMap<String,
		 * HashMap<String,Long>>(new ComparableKeys(2));
		 * 
		 * HashBucketTest bucket = new HashBucketTest(null , 0);
		 * 
		 * String[] keys = {"11100", "100", "1000", "10000", "10", "20", "30",
		 * "40", "11000"}; for( String key : keys) { bucket.putAddress(key,
		 * Long.valueOf(key)); }
		 * 
		 * List<Map< String, Long>> values = bucket.getAllValues("10");
		 * 
		 * for( Map<String, Long> map : values) { System.out.println("values:" +
		 * map.entrySet()); }
		 */
		/*try {
			String a1 = "1";
			String a2 = "01";

			System.out.println("" + a1.compareTo(a2));

			club club1 = new club();
			club1.add(20);
			club1.add(30);

			club club2 = new club();
			club2.add(30);
			club2.add(40);

			club club3 = new club();
			club3.add(80);
			club3.add(60);
			club3.add(70);

			HashBucketTest bucket1 = new HashBucketTest();
			bucket1.putAddress("0", 30);

			HashBucketTest bucket2 = new HashBucketTest();
			bucket2.putAddress("22312", 40);

			HashBucketTest bucket3 = new HashBucketTest();
			bucket3.putAddress("22312", 50);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oops = new ObjectOutputStream(baos);
			
			FileOutputStream fout = new FileOutputStream("objects.txt");
			BufferedOutputStream bs = new BufferedOutputStream(fout);
			
			int position1 = baos.size();								// the first object position
			bs.write(baos.toByteArray());								// write head
			baos.reset();
			
			oops.writeObject(bucket1);					// write the first object to byte array

			oops.reset();
			int position2 = position1 + baos.size();	// the second object position
			bs.write(baos.toByteArray());				// write first object to file
			baos.reset();
			
			oops.writeObject(bucket2);					// write the second object to byte array
			oops.reset();
			int position3 = position2 + baos.size();	// the third object position
			bs.write(baos.toByteArray());				// write the second object to file
			baos.reset();
			
			oops.writeObject(bucket3);					// write the third object to byte array
			oops.reset();
			int position4 = position3 + baos.size();	// the fourth object position
			bs.write(baos.toByteArray());				// write the third object to file
			baos.reset();

			bs.flush();
			bs.close();
			fout.flush();
			fout.close();

			// read according to the position x

			HashBucketTest readCase1 = null;
			HashBucketTest readCase2 = null;
			HashBucketTest readCase3 = null;
			
			FileInputStream fis = new FileInputStream("objects.txt");
			ObjectInputStream ois = new ObjectInputStream(
					fis);
			
			// success case
			readCase1 = (HashBucketTest) ois.readObject();	// read the first object, success
			fis.getChannel().position(position2);
			readCase2 = (HashBucketTest) ois.readObject();	// read the second object, success
			fis.getChannel().position(position3);
			readCase3 = (HashBucketTest) ois.readObject();	// read the third object, success
			
			// failed case!!!!
			readCase1 = (HashBucketTest) ois.readObject();	// read the first object, success
			fis.getChannel().position(position3);
			readCase3 = (HashBucketTest) ois.readObject();	// read the second object, success

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static <T extends Serializable> void writeSerializable(T serialized) {

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = null;

		try {
			objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			objectOutputStream.writeObject(serialized);
		} catch (Exception cause) {
			cause.printStackTrace();
		} finally {
			// closeStream(objectOutputStream);
		}

		FileOutputStream fout;
		try {
			fout = new FileOutputStream("club.txt", true);
			ObjectOutputStream oos = new ObjectOutputStream(
					new BufferedOutputStream(fout));
			oos.write(byteArrayOutputStream.toByteArray());
			System.out.println("position:"
					+ byteArrayOutputStream.toByteArray().length);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		 * RandomAccessFile serFile = null; FileChannel fileChannel = null;
		 * 
		 * try { byte[] source = byteArrayOutputStream.toByteArray(); serFile =
		 * new RandomAccessFile(file, "rw"); fileChannel = serFile.getChannel();
		 * ByteBuffer buffer = fileChannel.map(MapMode.READ_WRITE, 0,
		 * source.length); FileLock fileLock = fileChannel.lock();
		 * buffer.put(source); fileLock.release(); buffer.clear(); } catch
		 * (Exception cause) { cause.printStackTrace(); } finally {
		 * 
		 * }
		 
	}

	public static class club implements Serializable {
		int a = 0;
		int b = 0;
		Map<String, Map<String, member>> values = null;

		public club() {
			values = new TreeMap<String, Map<String, member>>();
		}

		public void add(int number) {
			Map<String, member> temp = values.get(String.valueOf(number));
			if (temp == null) {
				temp = new HashMap<String, member>();
				values.put(String.valueOf(number), temp);
			}
			temp.put(String.valueOf(number), new member(number));
		}

	}

	public static class member implements Serializable {
		int person = 0;

		public member() {
			person = 0;
		}

		public member(int person) {
			this.person = person;
		}
	}

	public static class HashBucketTest implements Serializable {

		private static final long serialVersionUID = 3610182543890121796L;

		Map<String, Integer> values = null; // need to write to disk

		public HashBucketTest() {

			values = new TreeMap<String, Integer>();
		}

		public void putAddress(String key, int number) {

			values.put(key, number);
		}

	}

	static class Key implements Serializable {
		private String keyString;
		static int xor = 0;

		Key(String keyString) {
			this.keyString = keyString;
		}

		@Override
		public int hashCode() {
			return keyString.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			Key otherKey = (Key) obj;
			return keyString.equals(otherKey.keyString);
		}*/

	}
	
	public static class OrderedList<T extends Comparable<T>> extends LinkedList<T> {

	    private static final long serialVersionUID = 1L;


	    public boolean orderedAdd(T element) {      
	        ListIterator<T> itr = listIterator();
	        while(true) {
	            if (itr.hasNext() == false) {
	                itr.add(element);
	                return(true);
	            }

	            T elementInList = itr.next();
	            if (elementInList.compareTo(element) > 0) {
	                itr.previous();
	                itr.add(element);
	                //System.out.println("Adding");
	                return(true);
	            }
	        }
	    }
	}
}
