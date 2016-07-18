package com.alibaba.middleware.handlefile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class TestOffset {

	BufferedReader reader;
	
	public TestOffset(){
		try {
			reader = new BufferedReader(new FileReader("buildfiles/order/order_0.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			reader.skip(1192903);
			System.out.println(reader.readLine());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]){
		TestOffset testOffset = new TestOffset();
	}
	
}
