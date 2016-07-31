package com.alibaba.middleware.disruptor;

import com.lmax.disruptor.EventHandler;

public class EventGCHandler implements EventHandler<RecordsEvent>{

	@Override
	public void onEvent(RecordsEvent event, long sequence, boolean endOfBatch)
			throws Exception {
		// TODO Auto-generated method stub
		event = null;
	}
	
}
