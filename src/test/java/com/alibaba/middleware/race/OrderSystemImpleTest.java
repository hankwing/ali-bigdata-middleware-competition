package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

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

    public static void main(String[] args) {
        construct();
        query();
    }

    public static void construct() {
        List<String> buyerfiles = new ArrayList<String>();
//        buyerfiles.add("benchmark/prerun_data/buyer.0.0");
//        buyerfiles.add("benchmark/prerun_data/buyer.1.1");
        buyerfiles.add("benchmark/buyer_records0.txt");

        List<String> goodfiles = new ArrayList<String>();
//        goodfiles.add("benchmark/prerun_data/good.0.0");
//        goodfiles.add("benchmark/prerun_data/good.1.1");
//        goodfiles.add("benchmark/prerun_data/good.2.2");
        goodfiles.add("benchmark/good_records0.txt");

        List<String> orderfiles = new ArrayList<String>();
//        orderfiles.add("benchmark/prerun_data/order.0.0");
//        orderfiles.add("benchmark/prerun_data/order.1.1");
//        orderfiles.add("benchmark/prerun_data/order.2.2");
//        orderfiles.add("benchmark/prerun_data/order.0.3");
        orderfiles.add("benchmark/order_records0.txt");

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
}
