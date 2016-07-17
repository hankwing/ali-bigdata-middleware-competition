package com.alibaba.middleware.handlefile;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class AgentMapping {
	// TODO change to ConcurrentHashMap
	ConcurrentHashMap<String, Integer> agentMapping;
	private int count;
	public AgentMapping() {
		agentMapping = new ConcurrentHashMap<String, Integer>();
		count = 0;
	}

	public void addEntry(String key) {
		agentMapping.put(key, new Integer(count));
		count++;
	}

	public int getCount() {
		return count;
	}

	public Integer getValue(String key){
		return agentMapping.get(key);
	}
}
