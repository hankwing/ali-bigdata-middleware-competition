package com.alibaba.middleware.handlefile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteFile {

	/**
	 * 文件最多纪录数为 MAX_LINES
	 * 文件的偏移量为 offset
	 * 文件纪录的计数 count
	 */
	private int MAX_LINES = 10000000;
	private long offset;
	private int count;

	private BufferedWriter writer;
	private String filePerfix;
	private String fileName;
	private int fileNum;

	public WriteFile(String path,String name, int maxLines) {
		this.offset = 0;
		this.count = 0;
		this.fileNum = 0;
		this.MAX_LINES = maxLines;

		//如果文件夹不存在则创建文件夹
		File file = new File(path);
		if (!file.exists()) {
			file.mkdirs();
		}

		filePerfix = new String(path + name);
		try {
			fileName = filePerfix + String.valueOf(fileNum) + ".txt";
			this.writer = new BufferedWriter(new FileWriter(fileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void writeLine(String line){
		try {
			if (count == MAX_LINES) {
				writer.close();
				//创建新的文件
				fileNum++;
				fileName = filePerfix + String.valueOf(fileNum) + ".txt";
				writer = new BufferedWriter(new FileWriter(fileName));
				offset = 0;
				count = 0;
			}

			writer.write(line+"\n");
			offset = offset + line.length() + 1;
			count++;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeFile(){
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public long getOffset() {
		return offset;
	}

	public String getFileName() {
		return fileName;
	}

}
