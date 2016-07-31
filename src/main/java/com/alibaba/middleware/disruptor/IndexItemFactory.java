package com.alibaba.middleware.disruptor;

import java.util.concurrent.ArrayBlockingQueue;

import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.handlefile.IndexItem;
import com.alibaba.middleware.index.DiskHashTable;
import com.lmax.disruptor.EventFactory;

public class IndexItemFactory implements EventFactory<IndexItem>{
	@Override
	public IndexItem newInstance() {
		// TODO Auto-generated method stub
		return new IndexItem();
	}

}
