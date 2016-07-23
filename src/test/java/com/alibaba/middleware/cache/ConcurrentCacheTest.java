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

        for (int i = 0; i < 10000; i++) {
            cache.putInCache(i, String.valueOf(i), RaceConfig.TableName.BuyerTable);
            cache.putInCache(i, String.valueOf(i), RaceConfig.TableName.GoodTable);
//            cache.putInCache(i, String.valueOf(i), RaceConfig.TableName.OrderTable);
        }

        cache.forceEvict(1000);

        System.out.println(cache.getSize());
    }
}
