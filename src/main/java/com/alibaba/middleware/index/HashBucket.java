package com.alibaba.middleware.index;

import java.io.Serializable;

import com.alibaba.middleware.conf.RaceConfig;

public class HashBucket implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3610182543890121796L;
	private int capacity  = 0;

	public HashBucket() {
		capacity = RaceConfig.hash_index_block_capacity;
	}
}
