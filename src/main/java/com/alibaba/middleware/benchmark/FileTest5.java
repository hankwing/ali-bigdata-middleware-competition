package com.alibaba.middleware.benchmark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class FileTest5 {
	
	/**
	 * 利用阻塞队列
	 */
	LinkedBlockingQueue<String> queue;

	public void test() {
		queue = new LinkedBlockingQueue<String>(10000);
		Thread th2 = new Thread(new Runnable() {
			@Override
			public void run() {
				List<String> files = new ArrayList<String>();
				files.add("buyer_records_1.txt");
				files.add("buyer_records_2.txt");
				files.add("good_records_1.txt");
				files.add("good_records_2.txt");
				for(String file:files){
					BufferedReader reader = null;
					try {
						reader = new BufferedReader(new FileReader(file));
						String record;
						record = reader.readLine();
						while (record!=null) {
							try {
								queue.put(record);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							record = reader.readLine();
						}
						reader.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
				try {
					queue.put("END");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		});
		th2.start();

		Thread th1 = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {
					BufferedWriter writer = new BufferedWriter(new FileWriter("haha.txt"));
					while(true){
						try {
							String record = queue.take();
							if (record.equals("END")) {
								break;
							}else {
								writer.write(record);
							}
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		th1.start();

		try {
			th1.join();
			th2.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String args[]){
		long start = System.currentTimeMillis();
		FileTest5 test5 = new FileTest5();
		test5.test();
		long end = System.currentTimeMillis();
		System.out.print(end - start);
	}
}
