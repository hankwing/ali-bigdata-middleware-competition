package com.alibaba.middleware.threads;

import com.alibaba.middleware.race.ResultImpl;

import java.util.Collection;

/**
 * @author Jelly
 */
public class QueryOrderThread extends QueryThread<ResultImpl> {
    private long orderId;
    private Collection<String> keys;

    public QueryOrderThread(long orderId, Collection<String> keys) {
        this.orderId = orderId;
        this.keys = keys;
    }

    @Override
    public ResultImpl call() throws Exception {
        // TODO
        return null;
    }

}
