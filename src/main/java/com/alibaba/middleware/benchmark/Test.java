package com.alibaba.middleware.benchmark;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.util.ArrayList;

/**
 * 不用管
 * @author hankwing
 *
 */
public class Test {

	public static void main( String[] args) {
		
		club club1 = new club();
		club1.add(20);
		club1.add(30);
		long position1 = 0;
		
		club club2 = new club();
		club2.add(30);
		club2.add(40);
		long position2 = 0;
		
		club club3 = new club();
		club3.add(50);
		club3.add(60);
		club3.add(70);
		long position3 = 0;
		
		FileOutputStream fout;
		FileInputStream streamIn;
		BufferedOutputStream bs;
		
		try {
			//fout = new FileOutputStream("club.txt");
			/*ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(fout));
			postition1 = fout.getChannel().position();
			oos.writeObject(newClub);*/

			//position2 = fout.getChannel().position();
			
			//oos.writeObject(club2);
			
			//position3 = fout.getChannel().position();
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
	        ObjectOutputStream objectOutputStream = null;

	        try {
	            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
	            objectOutputStream.writeObject(club1);

	            position1 += byteArrayOutputStream.size();
	            
	            fout = new FileOutputStream("club.txt", false);
				//ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(fout));
				//oos.writeObject(newClub);
	            bs = new BufferedOutputStream(fout);
	            bs.write(byteArrayOutputStream.toByteArray());
				System.out.println("position2:" + position1);
				
				byteArrayOutputStream.reset();
				objectOutputStream.writeObject(club2);
				position1 += byteArrayOutputStream.size();
	            
				bs.write(byteArrayOutputStream.toByteArray());
				System.out.println("position3:" + position1);
				
				byteArrayOutputStream.reset();
				objectOutputStream.writeObject(club3);
	            
				bs.write(byteArrayOutputStream.toByteArray());
				System.out.println("position:" + byteArrayOutputStream.toByteArray().length);
				
				bs.flush();
				bs.close();
	            
	        } catch (Exception cause) {
	            cause.printStackTrace();
	        } finally {
	            //closeStream(objectOutputStream);
	        	
	        }


			//oos.writeObject(club3);
			//oos.flush();
			//oos.close();

			streamIn = new FileInputStream("club.txt");
			ObjectInputStream objectinputstream = new ObjectInputStream(streamIn);
		    club readCase = null;
		    FileChannel channel = streamIn.getChannel();
		    channel.position(0);
		    readCase = (club) objectinputstream.readObject();
		    System.out.println(readCase.values.get(0).person);
		    
		    long postition2 = channel.position();
		    streamIn.getChannel().position(250);
		    readCase = (club) objectinputstream.readObject();
		    System.out.println(readCase.values.get(0).person);
			
			System.out.println("position:" + postition2);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static <T extends Serializable> void writeSerializable( T serialized) {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = null;

        try {
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(serialized);
        } catch (Exception cause) {
            cause.printStackTrace();
        } finally {
            //closeStream(objectOutputStream);
        }
        
        FileOutputStream fout;
		try {
			fout = new FileOutputStream("club.txt", true);
			ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(fout));
			oos.write(byteArrayOutputStream.toByteArray());
			System.out.println("position:" + byteArrayOutputStream.toByteArray().length);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

        /*RandomAccessFile serFile = null;
        FileChannel fileChannel = null;

        try {
            byte[] source = byteArrayOutputStream.toByteArray();
            serFile = new RandomAccessFile(file, "rw");
            fileChannel = serFile.getChannel();
            ByteBuffer buffer = fileChannel.map(MapMode.READ_WRITE, 0, source.length);
            FileLock fileLock = fileChannel.lock();
            buffer.put(source);
            fileLock.release();
            buffer.clear();
        } catch (Exception cause) {
            cause.printStackTrace();
        } finally {
        	
        }*/
    }
	
	public static class club implements Serializable{
		int a = 0;
		int b = 0;
		ArrayList<member> values = new ArrayList<member>();
		
		public club() {

		}
		
		public void add( int number) {
			values.add(new member(number));
		}
		

	}
	
	public static class member implements Serializable{
		int person  = 0;
		
		public member() {
			person = 0;
		}
		
		public member( int person) {
			this.person = person;
		}
	}
}
