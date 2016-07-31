//package com.alibaba.middleware;
//
//import me.lemire.integercompression.differential.IntegratedIntCompressor;
//
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.Random;
//import java.util.zip.DataFormatException;
//import java.util.zip.Deflater;
//import java.util.zip.Inflater;
//
///**
// * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
// *
// * @author Jelly
// */
//public class CompressionTest {
//
//    private static int longSize = Long.SIZE / Byte.SIZE;
//
//    public static byte[] compress(byte[] data) throws IOException {
//        Deflater deflater = new Deflater();
//        deflater.setInput(data);
//
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
//
//        deflater.finish();
//        byte buffer[] = new byte[10*8];
//        while (!deflater.finished()) {
//            int count = deflater.deflate(buffer);
//            outputStream.write(buffer, 0, count);
//        }
//        outputStream.close();
//        byte output[] = outputStream.toByteArray();
//
//        return output;
//    }
//
//    public static byte[] decompress(byte[] data) throws IOException, DataFormatException {
//        Inflater inflater = new Inflater();
//        inflater.setInput(data);
//
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
//        byte buffer[] = new byte[10*8];
//        while (!inflater.finished()) {
//            int count = inflater.inflate(buffer);
//            outputStream.write(buffer, 0, count);
//        }
//        outputStream.close();
//        byte output[] = outputStream.toByteArray();
//
//        return output;
//    }
//
//    public static void main(String[] args) {
////        Random random = new Random();
////        ByteBuffer byteBuffer = ByteBuffer.allocate(8*10);
////        System.out.println(longSize);
////        List<Long> longList = new ArrayList<>();
////        for (int i = 0; i < 10; i++) {
////            long value = random.nextLong();
////            longList.add(value);
////            System.out.println(value);
////        }
////        Collections.sort(longList);
////        System.out.println("Sorting");
////        for (int i = 0; i < 10; i++) {
////            System.out.println(longList.get(i));
////        }
////        for (int i = 0; i < 10; i++) {
////            byteBuffer.putLong(i, longList.get(i));
////        }
////        byte originB[] = byteBuffer.array();
////        System.out.println("Origin size: " + originB.length);
////
////        try {
////            byte compressed[] = compress(originB);
////            System.out.println("Compressed size: " + compressed.length);
////
////            byte decompressed[] = decompress(compressed);
////            ByteBuffer decompressedByteBuffer = ByteBuffer.wrap(decompressed);
////            for (int i = 0; i < 10; i++) {
////                System.out.println(decompressedByteBuffer.getLong(i*longSize) + "|" + longList.get(i));
////            }
////        } catch (IOException e) {
////            e.printStackTrace();
////        } catch (DataFormatException e) {
////            e.printStackTrace();
////        }
//
//        IntegratedIntCompressor iic = new IntegratedIntCompressor();
//        int num = 1000;
//        int[] intsA = new int[num];
//        Random random = new Random();
//        int counter = 0;
//        while (true) {
//            int v = random.nextInt();
//            if (v > 0) {
//                if (counter >= num) {
//                    break;
//                }
//                intsA[counter] = v;
//                counter++;
//            }
//        }
////            System.out.println(v);
//        for (int i = 0; i < num; i++) {
//            for (int j = 0; j < num-1; j++) {
//                if (intsA[j] > intsA[j+1]) {
//                    int temp = intsA[j+1];
//                    intsA[j+1] = intsA[j];
//                    intsA[j] = temp;
//                }
//            }
//        }
////        System.out.println("Sorted Done");
////        for (int i = 0; i < num; i++) {
////            System.out.println(intsA[i]);
////        }
//
//        int[] compressed = iic.compress(intsA);
//        int[] recov = iic.uncompress(compressed);
//        System.out.println("Compressed size: " + compressed.length);
//        for (int i = 0; i < 10; i++) {
//            // judge if equals
//            System.out.println(intsA[i] + "|" + recov[i]);
//        }
//    }
//}
