package com.alibaba.middleware.index;

import java.io.Serializable;
import java.util.Comparator;


public class ComparableKeys implements Comparator<String>, Serializable {

	int usedBits;

	public ComparableKeys(int usedBits) {
		this.usedBits = usedBits;
	}
	
	public ComparableKeys() {}

	@Override
	public int compare(String o1, String o2) {
		// TODO Auto-generated method stub
		o1 = o1.length() < usedBits? o1:o1.substring(o1.length() - usedBits);
		o2 = o2.length() < usedBits? o2:o2.substring(o2.length() - usedBits);
		return o1.compareTo(o2);
	}
}
