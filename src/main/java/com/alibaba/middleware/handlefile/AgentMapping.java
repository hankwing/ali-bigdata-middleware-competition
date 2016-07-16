package com.alibaba.middleware.handlefile;

import java.util.HashMap;

public class AgentMapping {
	HashMap<String, Integer> agentMapping;
	private int count;
	public AgentMapping() {
		agentMapping = new HashMap<String, Integer>();
		count = 0;
	}

	public synchronized void addEntry(String key) {
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
