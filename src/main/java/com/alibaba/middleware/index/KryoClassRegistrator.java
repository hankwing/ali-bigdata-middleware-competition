package com.alibaba.middleware.index;

import com.esotericsoftware.kryo.Kryo;

public interface KryoClassRegistrator {
	
	public void register(Kryo kryo);

}
