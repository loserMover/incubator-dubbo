/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.cache.support.jcache;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.TimeUnit;

/**
 * JCache
 */
public class JCache implements com.alibaba.dubbo.cache.Cache {
    /**
     * 缓存集合
     */
    private final Cache<Object, Object> store;

    public JCache(URL url) {
        //获得Cache key
        String method = url.getParameter(Constants.METHOD_KEY, "");
        String key = url.getAddress() + "." + url.getServiceKey() + "." + method;
        // jcache parameter is the full-qualified class name of SPI implementation
        //'jcache'配置项为JAVA SPI实现的全限定类名
        String type = url.getParameter("jcache");
        //基于类型，获得javax.cache.CachingProvider对象
        CachingProvider provider = type == null || type.length() == 0 ? Caching.getCachingProvider() : Caching.getCachingProvider(type);
        //获得javax.cache.CacheManager对象
        CacheManager cacheManager = provider.getCacheManager();
        //获得javax.cache.Cache对象
        Cache<Object, Object> cache = cacheManager.getCache(key);
        //不存在，则进行创建
        if (cache == null) {
            try {
                //configure the cache
                //设置Cache配置项
                MutableConfiguration config =
                        new MutableConfiguration<Object, Object>()
                                //类型
                                .setTypes(Object.class, Object.class)
                                //过期策略，按照写入时间过期。'cache.write.expire'配置项设置过期时间，默认为1分钟
                                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, url.getMethodParameter(method, "cache.write.expire", 60 * 1000))))
                                .setStoreByValue(false)
                                //设置MBean
                                .setManagementEnabled(true)
                                .setStatisticsEnabled(true);
                //创建javax.cache.Cache对象
                cache = cacheManager.createCache(key, config);
            } catch (CacheException e) {
                // concurrent cache initialization
                //初始化cache的并发情况
                cache = cacheManager.getCache(key);
            }
        }

        this.store = cache;
    }

    public void put(Object key, Object value) {
        store.put(key, value);
    }

    public Object get(Object key) {
        return store.get(key);
    }

}
