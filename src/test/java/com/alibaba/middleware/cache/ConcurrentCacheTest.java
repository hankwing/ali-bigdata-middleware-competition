package com.alibaba.middleware.cache;

import com.alibaba.middleware.conf.RaceConfig;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public class ConcurrentCacheTest {

    public static void main(String[] args) {
        ConcurrentCache cache = ConcurrentCache.getInstance();
        ConcurrentLinkedHashMap cacheMap = new ConcurrentLinkedHashMap.Builder<Integer, String>()
                .initialCapacity(5)
                .maximumWeightedCapacity(10)
                .build();

        for (int i = 0; i < 10001; i++) {
            cache.putInCache(i, String.valueOf(i), RaceConfig.TableName.BuyerTable);
            cache.putInCache(i, String.valueOf(i), RaceConfig.TableName.GoodTable);
            cache.putInCache(i, String.valueOf(i), RaceConfig.TableName.OrderTable);
//            cacheMap.put(i, String.valueOf(i));
        }
//        cache.putInCache(9, String.valueOf(9), RaceConfig.TableName.BuyerTable);
//        System.out.println(cache.getFromCache(9, RaceConfig.TableName.BuyerTable));
        cacheMap.put(11, "3");
        System.out.println(cacheMap.get(0));

//        System.out.println("Force eviction");
//        cache.forceEvict(1000000);

//        try {
//            Thread.sleep(4000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        for (int i = 0; i < 2000000000; i++) {
//            cache.putInCache(i, String.valueOf(i), RaceConfig.TableName.BuyerTable);
//            cache.putInCache(i, String.valueOf(i), RaceConfig.TableName.GoodTable);
//            cache.putInCache(i, String.valueOf(i), RaceConfig.TableName.OrderTable);
//        }

        System.out.println(cache.getSize());
    }
}
