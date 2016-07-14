package com.alibaba.middleware.index;

import java.io.Serializable;
import java.util.Comparator;


/**
 * 实现每个哈希桶内的根据后usedBits位聚簇存放  这样在新桶加入后可以很快定位需要加入到新桶的数据
 * @author hankwing
 *
 */
public class ComparableKeys implements Comparator<String>, Serializable {

	private static final long serialVersionUID = 4306303294520820664L;
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
