package com.alibaba.middleware.handlefile;

import java.util.concurrent.ConcurrentHashMap;

public class DataFileMapping {
	ConcurrentHashMap<Integer, String> dataFileMapping;
	int dataFileSerialNumber;
	
	public DataFileMapping() {
		dataFileMapping = new ConcurrentHashMap<Integer, String>();
		dataFileSerialNumber = 0;
	}
	
	public void addDataFile(String file){
		dataFileMapping.put(dataFileSerialNumber, file);
		dataFileSerialNumber++;
	}
	
	public String getDataFile(int dataFileSerialNumber){
		return dataFileMapping.get(dataFileSerialNumber);
	}
	
	public int getDataFileSerialNumber(){
		return dataFileSerialNumber;
	}
}
