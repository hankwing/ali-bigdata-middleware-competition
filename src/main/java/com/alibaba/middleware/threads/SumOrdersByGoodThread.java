package com.alibaba.middleware.threads;

import com.alibaba.middleware.race.KeyValueImpl;
import com.alibaba.middleware.race.OrderSystem.KeyValue;

/**
 * @author Jelly
 */
public class SumOrdersByGoodThread extends QueryThread<KeyValueImpl> {

    private String goodid;
    private String key;

    public SumOrdersByGoodThread(String goodid, String key) {
        this.goodid = goodid;
        this.key = key;
    }

    @Override
    public KeyValueImpl call() throws Exception {
        // TODO
        return null;
    }
}
