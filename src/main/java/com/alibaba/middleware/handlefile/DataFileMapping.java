package com.alibaba.middleware.handlefile;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DataFileMapping {
	ConcurrentHashMap<Integer, String> dataFileMapping;
	AtomicInteger dataFileSerialNumber;
	
	public DataFileMapping() {
		dataFileMapping = new ConcurrentHashMap<Integer, String>();
		dataFileSerialNumber = new AtomicInteger(0);
	}
	
	public synchronized int addDataFileName(String file){
		dataFileMapping.put(dataFileSerialNumber.get(), file);
		
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
