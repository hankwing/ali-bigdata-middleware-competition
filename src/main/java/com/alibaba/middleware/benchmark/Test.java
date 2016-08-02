package com.alibaba.middleware.benchmark;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.instrument.Instrumentation;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPOutputStream;

import com.alibaba.middleware.cache.ConcurrentCache;
import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.conf.RaceConfig.DirectMemoryType;
import com.alibaba.middleware.conf.RaceConfig.IdIndexType;
import com.alibaba.middleware.conf.RaceConfig.TableName;
import com.alibaba.middleware.index.ComparableKeys;
import com.alibaba.middleware.index.DiskHashTable;
import com.alibaba.middleware.index.HashBucket;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.tools.ByteUtils;
import com.alibaba.middleware.tools.RecordsUtils;

/**
 * 不用管
 * 
 * @author hankwing
 *
 */
public class Test {

	public static void main(String[] args) {

		// if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
		// System.out.println("BIG_ENDIAN");
		// } else {
		// System.out.println("LITTLE_ENDIAN");
		// }
		//
		// byte[] array = new byte[]{116, 101, 1, 6};
		//
		// int value = ByteUtils.byteArrayToLeInt(array);
		// System.out.println(value);
		
		String line = "见到榆树林：虽然新疆煤矿总医院，医务室善如：登宰产肉量。各章错划。斯皮尔斯黄智权，弗雷西内，用心功能，学生发表，味之素澳门中华教育会，铁件优惠。印度工业联合会弯弯曲曲：太阳站住：不甘示弱先畴。诡异嘉实：开始窈窕淑女。大通曼哈顿银行用处，艳阳双列，玉林日报赐福。尤康瑞星。直升机中关村电脑节组委会：石油伴生气站长，三无毓丹。如期票友。南沙滩美妙：穆尼蒂斯千。探究式翻晒：艺术节会理县：酶标仪纳希切万，杀人不眨眼朝三暮四：是原因。巴西翔华。昌平公司，期生规格全：管道设备前因后果：形式九派。同正实事求是。劳动保障部峻阻：案犯必不可少，预热学府。里科尔张晓丽，邦哲晓得：美术室退堂鼓：巴雷拉线性规划。俾路支陆桥：有令不行爆炸声，避免医疗美容。常值画船。给肚皮：小麦痰咳净，工作中文。贝尔维尔我们，商品材瓦连科：响象上海粮油商品交易所。出致和。莱切纪录片，钟老三元朱。大量贱生：张家窝镇无声无臭：挂彩血尿，电阻率敏强，发汗药救多善。不可卡万，统计公告乔乌。引擎南湖，\n";
		int length = line.getBytes().length;
		ByteBuffer buffer = ByteBuffer.wrap(line.getBytes());
		
		byte[] combineData = new byte[0];
		//StringBuilder builder = new StringBuilder();
		byte[] data = new byte[1024];
		int locateEnd = 0;
		boolean isEnd = true;
		while (true) {
			if (buffer.remaining() < data.length) {
				data = new byte[buffer.remaining()];
			}
			buffer.get(data);
			for (int i = 0; i < data.length; i++) {
				if (data[i] == '\n') {
					isEnd = false;
					locateEnd = i;
					break;
				}
			}
			if (isEnd == false) {
				break;
			}
			byte[] newByte = new byte[combineData.length+ data.length];  
	        System.arraycopy(combineData, 0, newByte, 0, combineData.length);  
	        System.arraycopy(data, 0, newByte, combineData.length, data.length);
	        combineData = newByte;
		}
		byte[] finalByte = new byte[combineData.length+ data.length];  
        System.arraycopy(combineData, 0, finalByte, 0, combineData.length);  
        System.arraycopy(data, 0, finalByte, combineData.length, data.length);
		System.out.println("read from direct mem:" + new String(finalByte));
		
		System.out.println(length);

	}

}
