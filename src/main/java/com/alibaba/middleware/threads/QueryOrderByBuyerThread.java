package com.alibaba.middleware.threads;

import com.alibaba.middleware.race.OrderSystem.Result;

import java.util.Iterator;

/**
 * @author Jelly
 */
public class QueryOrderByBuyerThread extends QueryThread<Iterator<Result>> {
    private long startTime;
    private long endTime;
    private String buyerid;

    public QueryOrderByBuyerThread(long startTime, long endTime, String buyerid) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.buyerid = buyerid;
    }

    @Override
    public Iterator<Result> call() throws Exception {
        // TODO
        return null;
    }
}
