package com.alibaba.middleware.handlefile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileLock;

public class WriteFile {

	private int MAX_LINES = 100;
	private int count;
	
	private BufferedWriter writer;
	private String fileName;
	private int fileNum;

	public WriteFile(String path,String name, int maxLines) {
		this.count = 0;
		this.fileNum = 0;
		File file = new File(path);
		if (!file.exists()) {
			file.mkdirs();
		}
		fileName = new String(path + name);
		try {
			this.writer = new BufferedWriter(new FileWriter(fileName + String.valueOf(fileNum) + ".txt"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.MAX_LINES = maxLines;
	}

	public synchronized void writeLine(String line){
		try {
			if (count == MAX_LINES) {
				writer.close();
				fileNum++;
				writer = new BufferedWriter(new FileWriter(fileName + String.valueOf(fileNum) + ".txt"));
				count = 0;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			writer.write(line+"\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		count++;
	}

	public void closeFile(){
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
