package com.alibaba.middleware.cache;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public class ConcurrentCacheTest {

    public static void main(String[] args) {
//        ConcurrentCache cache = ConcurrentCache.getInstance();
        try {
            System.out.println("Sleep...");
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ConcurrentLinkedHashMap cacheMap = new ConcurrentLinkedHashMap.Builder<Integer, String>()
                .initialCapacity(50)
                .maximumWeightedCapacity(1000000)
                .build();

        try {
            System.out.println("Sleep...");
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < 11; i++) {
//            List<byte[]> list = new ArrayList<byte[]>();
//            cache.putInCache(i, String.valueOf(i), RaceConfig.TableName.BuyerTable);
//            list.add("s".getBytes());
//            cache.putInIdCache(i, list, RaceConfig.IdIndexType.BuyerIdToOrderOffsets);
            cacheMap.put(i, String.valueOf(i));
        }
//        System.out.println(cache.getFromIdCache(1, RaceConfig.IdIndexType.BuyerIdToOrderOffsets));
//        cache.putInCache(9, String.valueOf(9), RaceConfig.TableName.BuyerTable);
//        System.out.println(cache.getFromCache(9, RaceConfig.TableName.BuyerTable));

//        System.out.println("Force eviction");
//        cache.forceEvict(1000000);
        System.out.println(cacheMap.get(1));
        System.out.println(cacheMap.get(0));
        System.out.println(cacheMap.get(10));

        try {
            System.out.println("Sleep...");
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 200000; i++) {
            cacheMap.put(i+10, String.valueOf(i+10));
        }
        try {
            System.out.println("Sleep...");
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 2000000000; i++) {
            cacheMap.put(i+200000, String.valueOf(i+200000));
        }

//        System.out.println(cache.getSize());
        System.out.println(cacheMap.size());
    }
}
