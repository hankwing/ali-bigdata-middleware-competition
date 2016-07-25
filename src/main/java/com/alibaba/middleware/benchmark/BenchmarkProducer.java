package com.alibaba.middleware.benchmark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

import com.alibaba.middleware.conf.RaceConfig;

/**
 * 生成订单信息表、买家信息表、商品表
 * 配置信息都在static字段里面
 * 
 * @author hankwing
 *
 */
public class BenchmarkProducer {

	public static String buyerTableFileName = "benchmark/buyer_records_1.txt";
	public static String buyerTableDatabaseFileName = "buyer_records_database.txt";
	
	public static String goodTableFileName = "benchmark/good_records_1.txt";
	public static String goodTableDatabaseFileName = "good_records_database.txt";
	
	public static String orderTableFileName = "benchmark/order_records_1.txt";
	public static String orderTableDatabaseFileName = "order_records_database.txt";
	
	public static long buyerTableRecordsNum = 7000000;
	public static long goodTableRecordsNum = 7000000;
	public static long orderTableRecordsNum = 10000;

	public static String buyerTableReAttr[] = {"buyerid","contactphone","recieveaddress"};
	public static String buerTableOpAttr[] = 
		{"app_buyer_112_0","app_buyer_41_0","app_buyer_76_0","app_buyer_74_0"};
	
	public static String goodTableReAttr[] = {"goodid","salerid","good_name","goodname"};
	public static String goodTableOpAttr[] = 
		{"app_good_112_8","app_good_33_3","app_good_3334_2","app_good_112_8","app_good_112_1"};
	
	public static String orderTableReAttr[] = {"orderid","goodid","buyerid","createtime","done","amount"};
	public static String orderTableOpAttr[] = 
		{"remark","app_order_76_1","app_order_33_2","app_order_76_0","app_order_10021_0"};
	
	public static void main( String[] args) {
		try {
			// produce buyer table
			/*smallTableProducer(buyerTableFileName, buyerTableDatabaseFileName,
					buyerTableRecordsNum, buyerTableReAttr, buerTableOpAttr);
			
			// produce good table
			smallTableProducer(goodTableFileName, goodTableDatabaseFileName,
					goodTableRecordsNum, goodTableReAttr, goodTableOpAttr);*/
			
			Random random = new Random();
			for( int i =5 ; i < 10; i++) {
				// then produce order table
				orderTableProducer("benchmark/order_records_"+ i + ".txt", orderTableDatabaseFileName,
						random.nextInt(60000000), orderTableReAttr, orderTableOpAttr);
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 产生good和buyer表
	 * 
	 * @param fileName
	 * @param databaseFileName
	 * @param recordsNum
	 * @param reAttr
	 * @param opAttr
	 * @throws IOException
	 */
	public static void smallTableProducer( String fileName, String databaseFileName, long recordsNum
			, String[] reAttr, String[] opAttr) throws IOException {
		
		MyRandom mRandom = new MyRandom();
		
		FileOutputStream fos = new FileOutputStream(fileName);
		FileOutputStream fosDatabase = new FileOutputStream(databaseFileName);
		Writer writer = new BufferedWriter(new OutputStreamWriter(fos));
		Writer writerDatabase = new BufferedWriter(new OutputStreamWriter(fosDatabase));

		while( recordsNum -- > 0) {
			
			StringBuilder records = new StringBuilder();
			StringBuilder recordsDatabase = new StringBuilder();
			// produce required attr
			for( String attrName : reAttr) {
				String randomValue = UUID.randomUUID().toString();
				records.append(attrName + ":" + randomValue + "\t");
				recordsDatabase.append(randomValue + "\t");
				
			}
			// produce optional attr
			for( String attrName : opAttr) {
				if( mRandom.nextBoolean()) {
					records.append(attrName + ":" + UUID.randomUUID().toString() + "\t");
				}
			}
			records.deleteCharAt(records.length() -1);
			records.append("\n");
			recordsDatabase.deleteCharAt(recordsDatabase.length() - 1);
			recordsDatabase.append("\n");
			
			writer.write(records.toString());
			writerDatabase.write(recordsDatabase.toString());
		}
		
		writer.flush();
		writer.close();
		writerDatabase.flush();
		writerDatabase.close();
		
	}
	
	/**
	 * 根据已产生的good和buyer表产生order表
	 * 
	 * @param fileName
	 * @param databaseFileName
	 * @param recordsNum
	 * @param reAttr
	 * @param opAttr
	 * @throws IOException
	 */
	public static void orderTableProducer( String fileName,
			String databaseFileName, long recordsNum, String[] reAttr, String[] opAttr) throws IOException {
		
		MyRandom mRandom = new MyRandom();
		
		FileOutputStream fos = new FileOutputStream(fileName);
		FileOutputStream fosDatabase = new FileOutputStream(databaseFileName);
		Writer writer = new BufferedWriter(new OutputStreamWriter(fos));
		//Writer writerDatabase = new BufferedWriter(new OutputStreamWriter(fosDatabase));
		
		// read buyerid from buyer table
		ArrayList<String> buyerIds = new ArrayList<String>();
		FileInputStream fis = new FileInputStream(buyerTableFileName);
	    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	    String buyerLine = null;
	    while( (buyerLine = br.readLine()) != null) {
	    	// get buyer id
	    	String[] kvs = buyerLine.split("\t");
	    	buyerIds.add(kvs[0].substring(kvs[0].indexOf(":") + 1));
	    }
	    
		// read goodid from good table
		ArrayList<String> goodIds = new ArrayList<String>();
		FileInputStream goodFis = new FileInputStream(goodTableFileName);
	    BufferedReader goodBr = new BufferedReader(new InputStreamReader(goodFis));
	    String goodLine = null;
	    while( (goodLine = goodBr.readLine()) != null) {
	    	// get buyer id
	    	String[] kvs = goodLine.split("\t");
	    	goodIds.add(kvs[0].substring(kvs[0].indexOf(":") + 1));
	    }

		while( recordsNum -- > 0) {
			
			StringBuilder records = new StringBuilder();
			StringBuilder recordsDatabase = new StringBuilder();
			// produce required attr
			for( String attrName : reAttr) {
				if( attrName.equals("orderid") || attrName.equals("createtime")) {
					Long value = Math.abs(mRandom.nextLong());
					records.append(attrName + ":" + value + "\t");
					recordsDatabase.append( value + "\t");
				}
				else if( attrName.equals("buyerid")) {
					String value = buyerIds.get( mRandom.nextInt(buyerIds.size()));
					records.append(attrName + ":" + value + "\t");
					recordsDatabase.append( value + "\t");
				}
				else if( attrName.equals("goodid")) {
					String value = goodIds.get( mRandom.nextInt(goodIds.size()));
					records.append(attrName + ":" + value + "\t");
					recordsDatabase.append( value + "\t");
				}
				else if( attrName.equals("done")) {
					String booleanValue = mRandom.nextBoolean() ? RaceConfig.booleanTrueValue : 
						RaceConfig.booleanFalseValue;
					records.append(attrName + ":" + booleanValue + "\t");
					recordsDatabase.append( booleanValue + "\t");
				}
				else if( attrName.equals("amount")) {
					Double value = Math.abs( mRandom.nextDouble() * mRandom.nextInt(200000));
					records.append(attrName + ":" + value.toString() + "\t");
					recordsDatabase.append( value.toString() + "\t");
				}
				
			}
			// produce optional attr
			for( String attrName : opAttr) {
				if( mRandom.nextBoolean()) {
					records.append(attrName + ":" + String.valueOf(mRandom.nextDouble()) + "\t");
				}
			}
			records.deleteCharAt(records.length() -1);
			records.append("\n");
			recordsDatabase.deleteCharAt(recordsDatabase.length() - 1);
			recordsDatabase.append("\n");
			
			writer.write(records.toString());
			//writerDatabase.write(recordsDatabase.toString());
		}
		
		writer.flush();
		writer.close();
		//writerDatabase.flush();
		//writerDatabase.close();
		
	}
	
	/**
	 * 可生成非负整数、可选字段个数、可选字段索引号
	 * @author hankwing
	 *
	 */
	public static class MyRandom extends Random {
		
		private static final long serialVersionUID = -5443526123917561568L;

		public MyRandom() {}
	    public MyRandom(int seed) { super(seed); }

	    public int nextNonNegative() {
	        return next(Integer.SIZE - 1);
	    }
	    
	    /**
	     * 返回有几个可选字段
	     * @param max
	     * @return
	     */
	    public int nextOptionalNum( int max) {
	    	return this.nextInt(max);
	    }
	    
	    /**
	     * 返回可选字段的下标索引
	     * @param optionalNum
	     * @return
	     */
	    public int nextPossible( int optionalNum) {
	    	return this.nextInt(optionalNum);
	    }
	}
}
