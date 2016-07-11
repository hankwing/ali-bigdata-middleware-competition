package com.alibaba.middleware.benchmark;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.alibaba.middleware.benchmark.Test.club;
import com.alibaba.middleware.index.DiskHashTable;

/**
 * 索引测试类   
 * 
 * 大量使用方法：new DiskHashTable<K, V> --> 调用put函数放key和value对 --> 参考main里面的"construct"命令 --> 
 * 			索引建完调用write方法将索引元数据写到文件里
 * 
 * 果冻使用方法：1.查找键  从文件中读取DiskHashTable(参考read命令) --> 调用lookup:xxx即可查找键对应的value
 * 			2.缓冲区管理  调用DiskHashTable的writeBucket方法即可将某个索引块写出去  释放缓冲区(建索引的同时也可写出去)
 * 			3.索引使用完后close
 * 
 * @author hankwing
 *
 */
public class IndexTest {

	public static void main( String[] args) {
		
		//Map<String, Integer> map = new HashMap<String, Integer>();
		BufferedReader br = null;
		DiskHashTable hashTable = null;
		
		FileOutputStream fout = null;
		FileInputStream streamIn = null;
		
		try {

		    Scanner scanner = new Scanner(System.in);
		    String command = null;
		    while( !(command = scanner.nextLine()).equals("quit")) {
		    	
		    	if( command.equals("write")) {
		    		// write 将内存中的索引文件写出去
		    		
		    		long startTime = System.currentTimeMillis();
		    		hashTable.writeAllBuckets();

	    			fout = new FileOutputStream("index.txt");
	    			ObjectOutputStream oos = new ObjectOutputStream(fout);
	    			oos.writeObject(hashTable);
	    			oos.close();
	    			System.out.println("write all bucket complete:"
	    			+ (System.currentTimeMillis() - startTime) / 1000);
		    		
		    	}
		    	else if( command.startsWith("writeBucket")) {
		    		// writeBucket:xx 将某个索引块xx写到外存  索引块号xx可在索引块内找到
		    		
		    		long startTime = System.currentTimeMillis();
		    		hashTable.writeBucket(Integer.valueOf( command.substring(command.indexOf(":") + 1)));
	    			System.out.println("write bucket complete:" +
		    		(System.currentTimeMillis() - startTime) / 1000);
		    	}
		    	else if(command.equals("read")) {
		    		// read 从文件中读取索引元数据DiskHashTable
		    		
		    		streamIn = new FileInputStream("index.txt");
					ObjectInputStream objectinputstream = new ObjectInputStream(streamIn);
				    
				    hashTable = (DiskHashTable) objectinputstream.readObject();
				    hashTable.restore();
				    objectinputstream.close();
				    System.out.println("read complete");
				    
		    	}
		    	else if( command.equals("construct")) {
		    		// 在内存中建立orderBench.txt的索引  建立期间可随时调用write将某个块写出去
		    		
		    		long startTime = System.currentTimeMillis();
		    		br = new BufferedReader(new FileReader("orderBench.txt"));
				    String line = br.readLine();
				    hashTable = new DiskHashTable("bucketFile","dataFile");
				    
				    while (line != null) {
				        String[] values = line.split("\\s+?");
				        String orderid = values[0].substring(values[0].indexOf(":") + 1);
				        long value = Long.valueOf( values[1].substring(values[1].indexOf(":") + 1));
				        //StorageAddress address = new StorageAddress();
				        //address.setString(value);
				        //map.put(orderid, address.fileOffset);
				        if( !hashTable.put(orderid, value)) {
				        	System.out.println("insert failure!");
				        }
				        
				        //System.out.println(orderid);
				        line = br.readLine();
				    	
				    }
				    System.out.println("constructing complete:" + (System.currentTimeMillis() - startTime) / 1000);
		    	}
		    	else if(command.startsWith("lookup")){
		    		// lookup:xxx 查找某个key值的value
		    		
		    		System.out.println("values:" + hashTable.get(command.substring(command.indexOf(":") + 1)));
		    	}
		    	else if( command.equals("quit")) {
		    		// 索引使用完毕 退出
		    		hashTable.cleanup();
		    	}
		    }
		    
		    scanner.close();
		    
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
