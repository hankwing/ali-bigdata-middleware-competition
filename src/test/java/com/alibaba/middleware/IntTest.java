package com.alibaba.middleware;

import me.lemire.integercompression.differential.IntegratedIntCompressor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public class IntTest {

    public static void main(String[] args) {
        IntegratedIntCompressor compressor = new IntegratedIntCompressor();
        int num = 1024;
        int[] array = new int[num];
        Random random = new Random();

        for (int i = 0; i < num; i++) {
            int v = random.nextInt();
//            System.out.println(v);
            array[i] = v;
        }

        int[] compressed = compressor.compress(array);
        System.out.println("Compressed size: " + compressed.length);

        int[] recov = compressor.uncompress(compressed);

        for (int i = 0; i < num; i++) {
//            System.out.println(recov[i]);
        }
    }
}
