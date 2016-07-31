package com.alibaba.middleware;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public class CompressionTest {

    private static int longSize = Long.SIZE / Byte.SIZE;

    public static byte[] compress(byte[] data) throws IOException {
        Deflater deflater = new Deflater();
        deflater.setInput(data);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);

        deflater.finish();
        byte buffer[] = new byte[10*8];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        byte output[] = outputStream.toByteArray();

        return output;
    }

    public static byte[] decompress(byte[] data) throws IOException, DataFormatException {
        Inflater inflater = new Inflater();
        inflater.setInput(data);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        byte buffer[] = new byte[10*8];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        byte output[] = outputStream.toByteArray();

        return output;
    }

    public static void main(String[] args) {
        Random random = new Random();
        ByteBuffer byteBuffer = ByteBuffer.allocate(8*10);
        System.out.println(longSize);
        List<Long> longList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            long value = random.nextLong();
            longList.add(value);
            System.out.println(value);
        }
        Collections.sort(longList);
        System.out.println("Sorting");
        for (int i = 0; i < 10; i++) {
            System.out.println(longList.get(i));
        }
        for (int i = 0; i < 10; i++) {
            byteBuffer.putLong(i, longList.get(i));
        }
        byte originB[] = byteBuffer.array();
        System.out.println("Origin size: " + originB.length);

        try {
            byte compressed[] = compress(originB);
            System.out.println("Compressed size: " + compressed.length);

            byte decompressed[] = decompress(compressed);
            ByteBuffer decompressedByteBuffer = ByteBuffer.wrap(decompressed);
            for (int i = 0; i < 10; i++) {
                System.out.println(decompressedByteBuffer.getLong(i*longSize) + "|" + longList.get(i));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (DataFormatException e) {
            e.printStackTrace();
        }
    }
}
