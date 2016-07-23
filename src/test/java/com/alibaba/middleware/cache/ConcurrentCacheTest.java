package com.alibaba.middleware.cache;

import com.alibaba.middleware.conf.RaceConfig;

/**
 * order-system-impl-master-cabe626d3eb46a36ae1f74a33ef3e8c7182536c7order-system-impl.git
 *
 * @author Jelly
 */
public class ConcurrentCacheTest {

    public static void main(String[] args) {
        ConcurrentCache cache = new ConcurrentCache();

        for (long i = 0; i < 10000; i++) {
            cache.putInCache(i, String.valueOf(i), RaceConfig.TableName.BuyerTable);
        }
    }
}
