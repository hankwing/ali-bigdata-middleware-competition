package com.alibaba.middleware.handlefile;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.tools.ByteUtils;

public class DataFileMapping {
	ConcurrentHashMap<Integer, String> dataFileMapping;
	AtomicInteger dataFileSerialNumber;
	
	public DataFileMapping() {
		dataFileMapping = new ConcurrentHashMap<Integer, String>();
		dataFileSerialNumber = new AtomicInteger(0);
	}
	
	public synchronized int addDataFileName(String file){
		if( !dataFileMapping.containsValue(file) ) {
			dataFileMapping.put(dataFileSerialNumber.get(), file);
		}
		else {
			// find the key
			for( int key : dataFileMapping.keySet().toArray(new Integer[0])) {
				if( dataFileMapping.get(key).equals(file)) {
					return key;
				}
			}
		}
		
		return dataFileSerialNumber.getAndIncrement();
	}
	
	public String getDataFileName(int dataFileSerialNumber){
		return dataFileMapping.get(dataFileSerialNumber);
	}
	
	public Integer[] getAllFileIndexs() {
		return dataFileMapping.keySet().toArray(new Integer[0]);
	}
	
	/*public AtomicInteger getDataFileSerialNumber(){
		return dataFileSerialNumber;
	}*/
}
