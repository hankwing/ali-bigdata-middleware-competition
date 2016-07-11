package com.alibaba.middleware.index;

import java.util.List;


public class ComparableKeys implements Comparable<ComparableKeys> {

	String key;
	int usedBits;

	public ComparableKeys(String key, int usedBits) {
		this.key = key;
		this.usedBits = usedBits;
	}

	public int compareTo(ComparableKeys o) {

		return Long.valueOf( new StringBuilder(key).reverse().toString()).
				compareTo(Long.valueOf( new StringBuilder(o.key).reverse().toString()));
	}
}
