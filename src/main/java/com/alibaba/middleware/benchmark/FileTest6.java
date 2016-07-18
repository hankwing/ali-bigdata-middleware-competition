package com.alibaba.middleware.benchmark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class FileTest6 {
	//利用缓冲区
	List<String> list = new ArrayList<String>();

	public void test() {
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
					list.add(record);
					record = reader.readLine();
				}

				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter("haha.txt"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (int i = 0; i < list.size(); i++) {
			try {
				writer.write(list.get(i));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
