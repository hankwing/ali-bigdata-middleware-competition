package com.alibaba.middleware.handlefile;

import com.alibaba.middleware.threads.WorkerThread;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

//采用多线程读文件
class ReadFile extends WorkerThread {
	ReadBlockingQueue queues;
//	CountDownLatch countDownLatch;
	List<String> files;

	public ReadFile(ReadBlockingQueue queues,
			CountDownLatch countDownLatch,
			List<String> files) {
		this.queues = queues;
//		this.countDownLatch = countDownLatch;
		this.files = files;
	}

	public ReadFile(ReadBlockingQueue queues, List<String> files) {
		this.queues = queues;
		this.files = files;
	}

	@Override
	public void run() {
		System.out.println("start reading!");
		for (String file : files) {
			BufferedReader bfr = null;
			try {
				bfr = new BufferedReader(new FileReader(file));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			String line = null;
			try {
				line = bfr.readLine();
				while (line != null) {
					queues.putEntry(line);
					line = bfr.readLine();
				}
				bfr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		queues.setEnd();
//		countDownLatch.countDown();
		System.out.println("end reading!");
	}

	@Override
	public String getWorkerName() {
		return "reader";
	}

	@Override
	public void setReadyToStop() {
	}

	@Override
	public boolean readyToStop() {
		return false;
	}
}