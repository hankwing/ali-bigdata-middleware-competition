package com.alibaba.middleware.handlefile;

import java.util.HashMap;

public class AgentMapping {
	HashMap<String, Long> agentMapping;
	Long count;
	public AgentMapping() {
		agentMapping = new HashMap<String, Long>();
		count = new Long(0);
	}

	public synchronized void addEntry(String key) {
		agentMapping.put(key, new Long(count));
		count++;
	}

	public Long getCount() {
		return count;
	}

	public Long getValue(String key){
		return agentMapping.get(key);
	}
}
