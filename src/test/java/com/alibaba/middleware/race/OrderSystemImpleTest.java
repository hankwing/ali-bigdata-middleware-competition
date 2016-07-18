package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public class OrderSystemImpleTest {
    static OrderSystemImpl orderSystem = new OrderSystemImpl();

    public static void main(String[] args) {
        construct();
        query();
    }

    public static void construct() {
        List<String> buyerfiles = new ArrayList<String>();
        buyerfiles.add("benchmark/buyer_records.txt");

        List<String> goodfiles = new ArrayList<String>();
        goodfiles.add("benchmark/good_records.txt");

        List<String> orderfiles = new ArrayList<String>();
        orderfiles.add("benchmark/order_records.txt");

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
    }
}
