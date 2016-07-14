package com.alibaba.middleware.tools;

import java.io.Serializable;

/**
 * 暂不用
 * @author hankwing
 *
 */
public class IndexBucketKey implements Serializable {

	private static final long serialVersionUID = -6787216426914099664L;
	public String key = null;
	
	public IndexBucketKey( String key) {
		this.key = key;
	}
	
	@Override
    public int hashCode()
    {
        return key.hashCode();
    }
	
	@Override
    public boolean equals(Object obj)
    {
		IndexBucketKey otherKey = (IndexBucketKey) obj;
        return key.equals(otherKey.key);
    }
}
