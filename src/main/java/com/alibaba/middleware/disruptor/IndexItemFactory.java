package com.alibaba.middleware.disruptor;

import java.util.concurrent.ArrayBlockingQueue;

import com.alibaba.middleware.conf.RaceConfig.IndexType;
import com.alibaba.middleware.handlefile.IndexItem;
import com.alibaba.middleware.index.DiskHashTable;
import com.lmax.disruptor.EventFactory;

public class IndexItemFactory implements EventFactory<RecordsEvent>{
	@Override
	public RecordsEvent newInstance() {
		// TODO Auto-generated method stub
		return new RecordsEvent();
	}

}
