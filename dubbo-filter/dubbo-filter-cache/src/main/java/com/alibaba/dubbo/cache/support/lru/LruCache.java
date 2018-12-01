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
package com.alibaba.dubbo.cache.support.lru;

import com.alibaba.dubbo.cache.Cache;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.LRUCache;

import java.util.Map;

/**
 * LruCache
 */
public class LruCache implements Cache {
    /**
     * 缓存集合
     */
    private final Map<Object, Object> store;

    public LruCache(URL url) {
        //'cache.size'配置项，设置缓存大小
        final int max = url.getParameter("cache.size", 1000);
        //创建LRUCache对象
        this.store = new LRUCache<Object, Object>(max);
    }

    public void put(Object key, Object value) {
        store.put(key, value);
    }

    public Object get(Object key) {
        return store.get(key);
    }

}
