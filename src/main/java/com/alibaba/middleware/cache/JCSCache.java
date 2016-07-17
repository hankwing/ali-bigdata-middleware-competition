//package com.alibaba.middleware.cache;
//
//import com.alibaba.middleware.conf.RaceConfig;
//import org.apache.commons.jcs.JCS;
//import org.apache.commons.jcs.access.CacheAccess;
//import org.apache.commons.jcs.access.exception.CacheException;
//import org.apache.commons.jcs.engine.control.CompositeCacheManager;
//
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.util.Properties;
//
///**
// * @author Jelly
// *
// * Cache implemented in JCS.
// * Modify config file to specify max object num and disk file path. Config file is specify in {@link RaceConfig}
// * {@literal jcs.auxiliary.DC.attributes.DiskPath}
// * {@literal jcs.region.commonCache.cacheattributes.MaxObjects}
// */
//public class JCSCache<K, V> implements Cache<K, V> {
//    private CacheAccess<K, V> cache = null;
//    private CompositeCacheManager cacheManager = CompositeCacheManager.getUnconfiguredInstance();
//
//    public JCSCache() {
//        Properties props = new Properties();
//        try {
//            props.load(new FileInputStream(RaceConfig.cacheConfig));
//            cacheManager.configure(props);
//
//            cache = JCS.getInstance("commonCache");
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public V getFromCache(K key) {
//        return cache.get(key);
//    }
//
//    public void putInCache(K key, V value) {
//        try {
//            cache.put(key, value);
//        } catch (CacheException e) {
//            System.out.println(String.format("Problem putting object %s in the cache, %s",
//                    key, e.getMessage()));
//        }
//    }
//}
