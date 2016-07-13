package com.alibaba.middleware.cache;

import com.alibaba.middleware.conf.RaceConfig;
import com.alibaba.middleware.index.HashBucket;
import org.apache.commons.jcs.JCS;
import org.apache.commons.jcs.access.CacheAccess;
import org.apache.commons.jcs.access.exception.CacheException;
import org.apache.commons.jcs.engine.control.CompositeCacheManager;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Jelly
 */
public class CachePool {
    // cache pool for index buckets
    private CacheAccess<Integer, HashBucket> indexCache = null;
    // cache pool for records
    private CacheAccess<String, Object> commonCache = null;
    private CompositeCacheManager cacheManager = CompositeCacheManager.getUnconfiguredInstance();

    public CachePool() {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(RaceConfig.cacheConfig));
            cacheManager.configure(props);

            indexCache = JCS.getInstance("indexCache");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putIndexCache(HashBucket bucket) {
        try {
            indexCache.put(bucket.getBucketKey(), bucket);
        }
        catch ( CacheException e ) {
            System.out.println(String.format("Problem putting bucket %s in the cache, %s",
                    bucket.getBucketKey(), e.getMessage()));
        }
    }

    public HashBucket getIndexCache(int bucketKey) {
        return indexCache.get(bucketKey);
    }

    public void putCommonCache(Object obj) {
        try {
            // TODO
        }
        catch ( CacheException e ) {
            // TODO
        }
    }

    public Object getCommonCache(String key) {
        // TODO
        return commonCache.get(key);
    }
}
