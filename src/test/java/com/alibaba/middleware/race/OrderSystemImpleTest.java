package com.alibaba.middleware.race;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.tools.RecordsUtils;

import java.io.*;
import java.util.*;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public class OrderSystemImpleTest {

    /**
     * -XX:InitialHeapSize=5368709120 -XX:MaxHeapSize=5368709120 -XX:MaxNewSize=1789571072 -XX:MaxTenuringThreshold=6 -XX:NewSize=1789571072 -XX:OldPLABSize=16 -XX:OldSize=3579138048 -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseParNewGC
     */
    static OrderSystemImpl orderSystem = new OrderSystemImpl();
    static List<String> buyerfiles = new ArrayList<String>();
    static List<String> goodfiles = new ArrayList<String>();
    static List<String> orderfiles = new ArrayList<String>();

    public static void main(String[] args) {
        construct();
        try {
            query2();
            System.out.println("Done query2");
            query3();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void construct() {
//        buyerfiles.add("benchmark/prerun_data/buyer.0.0");
//        buyerfiles.add("benchmark/prerun_data/buyer.1.1");
        buyerfiles.add("benchmark/buyer_records0");
        buyerfiles.add("benchmark/buyer_records1");

//        goodfiles.add("benchmark/prerun_data/good.0.0");
//        goodfiles.add("benchmark/prerun_data/good.1.1");
//        goodfiles.add("benchmark/prerun_data/good.2.2");
        goodfiles.add("benchmark/good_records0");
        goodfiles.add("benchmark/good_records1");

//        orderfiles.add("benchmark/prerun_data/order.0.0");
//        orderfiles.add("benchmark/prerun_data/order.1.1");
//        orderfiles.add("benchmark/prerun_data/order.2.2");
//        orderfiles.add("benchmark/prerun_data/order.0.3");
        orderfiles.add("benchmark/order_records00");
        orderfiles.add("benchmark/order_records01");
        orderfiles.add("benchmark/order_records02");
        orderfiles.add("benchmark/order_records10");
        orderfiles.add("benchmark/order_records11");
        orderfiles.add("benchmark/order_records12");

        List<String> storeFolders = new ArrayList<String>();
        // 添加三个盘符
        storeFolders.add("folder1/");
        storeFolders.add("folder2/");
        storeFolders.add("folder3/");

        try {
            orderSystem.construct(orderfiles, buyerfiles, goodfiles,
                    storeFolders);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void query() {
        Collection<String> keys = new ArrayList<String>();
        keys.add("done");
        keys.add("amount");
        System.out.println(orderSystem.queryOrder(7381492278246634845L, keys));
        keys = null;
        System.out.println(orderSystem.queryOrder(7381492278246634845L, keys));
        keys = new ArrayList<String>();
        System.out.println(orderSystem.queryOrder(7381492278246634845L, keys));

        Iterator<OrderSystem.Result> iterator = orderSystem.queryOrdersByBuyer(2616152755183780199L, 163089172349893490L, "35856dc2-9255-4379-a1c9-4a67f84f3c7b");
        while (iterator.hasNext()) {
            System.out.println((ResultImpl)iterator.next());
        }

        String goodId = "8ff6c8b6-147f-4962-ae1c-3342523823bd";
        String salerId = "bc0eee35-b7e8-484a-a909-ffa3f2cab50b";

        keys = null;
        iterator = orderSystem.queryOrdersBySaler(salerId, goodId, keys);
        while (iterator.hasNext()) {
            System.out.println((ResultImpl)iterator.next());
        }
    }

    public static void query2() throws IOException {
        System.out.println("start query2" );

        Random random = new Random();
        FileInputStream fis = new FileInputStream(buyerfiles.get(
                random.nextInt(buyerfiles.size())));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        for( int i = 0; i< 2000; i++) {
            String buyerId = RecordsUtils.getValueFromLine(br.readLine(), RaceConfig.buyerId);
            buyerId = buyerId == null? UUID.randomUUID().toString():buyerId;
            long startTime = random.nextLong();
            long endTime = random.nextLong();

            Iterator<OrderSystem.Result> results = orderSystem.queryOrdersByBuyer(startTime, endTime, buyerId);
            System.out.println("query2");
            while(results.hasNext()) {
                System.out.println("values:" + results.next());
            }
        }
        System.out.println("end query2");
        br.close();
    }

    public static void query3() throws IOException {
        System.out.println("start query3" );
        Random random = new Random();
        FileInputStream fis = new FileInputStream(goodfiles.get(random.nextInt(
                goodfiles.size())));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        for( int i = 0; i< 2000; i++) {
            String goodId = RecordsUtils.getValueFromLine(br.readLine(), RaceConfig.goodId);
            goodId = goodId == null? UUID.randomUUID().toString(): goodId;
            Iterator<OrderSystem.Result> results = orderSystem.queryOrdersBySaler("", goodId, null);
            System.out.println("query3");
            if(results.hasNext()) {
                System.out.println("values:" + results.next());
            }

        }
        System.out.println("stop query3" );
        br.close();
    }
}
