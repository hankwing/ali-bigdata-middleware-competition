package com.alibaba.middleware.handlefile;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class ReadBlockingQueue {
	//阻塞队列
	private final int count;
	private List<LinkedBlockingQueue<String>> queues;

	public ReadBlockingQueue(int count,int size) {
		//初始化阻塞队列
		this.count = count;
		queues = new ArrayList<LinkedBlockingQueue<String>>();
		for (int i = 0; i < count; i++) {
			queues.add(new LinkedBlockingQueue<String>(size));
		}
	}

	public synchronized void putEntry(String entry){
		try {
			queues.get((int)(Math.random()*count)).put(entry);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String getEntry(int locate){
		String entry = null;
		try {
			entry = queues.get(locate).take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return entry;
	}

	public LinkedBlockingQueue<String> getBlockQueue(int locate){
		return queues.get(locate);
	}

	public void setEnd(){
		for (int i = 0; i < count; i++) {
			try {
				queues.get(i).put("END");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
