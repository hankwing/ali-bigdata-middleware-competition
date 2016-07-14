package com.alibaba.middleware.benchmark;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

/**
 * 产生orderID和一个字段的模拟数据
 * @author hankwing
 *
 */
public class OrderBechProducer {

	
	public static void main( String[] args) {
		
		Writer writer = null;
		FileOutputStream fos = null;
		MyRandom random = new MyRandom();
		try {
			fos = new FileOutputStream("orderBench.txt");
			writer = new BufferedWriter(
					new OutputStreamWriter(fos));
			ArrayList<String> test = new ArrayList<String>();
			long orders = 500000;
			while( orders -- > 0) {
				String randomString = String.valueOf( random.nextInt());
				test.add(randomString);
				writer.write("orderid:" + randomString 
						+ " fileOffset:" + random.nextNonNegative() + "\n");
			}
			
			
			for( int i = 0; i < 3; i++ ) {
				orders = 500000;
				while( orders -- > 0) {
					writer.write("orderid:" + test.get((int) orders)  + " fileOffset:" + random.nextNonNegative() + "\n");
					
				}
			}
						
			writer.flush();
			writer.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
	public static class MyRandom extends Random {
	    public MyRandom() {}
	    public MyRandom(int seed) { super(seed); }

	    public int nextNonNegative() {
	        return next(Integer.SIZE - 1);
	    }
	}
	
}
