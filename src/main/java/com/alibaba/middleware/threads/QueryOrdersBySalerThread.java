package com.alibaba.middleware.threads;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystem.Result;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Jelly
 */
public class QueryOrdersBySalerThread extends QueryThread<Iterator<Result>> {
    private String salerid;
    private String goodid;
    private Collection<String> keys;

    public QueryOrdersBySalerThread(String salerid, String goodid, Collection<String> keys) {
        this.salerid = salerid;
        this.goodid = goodid;
        this.keys = keys;
    }

    @Override
    public Iterator<Result> call() throws Exception {
        // TODO
        return null;
    }
}
