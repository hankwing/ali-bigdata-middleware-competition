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


	}

}
