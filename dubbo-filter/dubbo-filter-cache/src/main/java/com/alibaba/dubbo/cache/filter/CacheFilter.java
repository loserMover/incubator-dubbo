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
package com.alibaba.dubbo.cache.filter;

import com.alibaba.dubbo.cache.Cache;
import com.alibaba.dubbo.cache.CacheFactory;
import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcResult;

/**
 * 用于服务消费者和提供者，提供结果缓存的功能。结果缓存，用于加速热门数据的访问，Dubbo服务提供声明式缓存，以减少用户加缓存的工作量
 * Dubbo提供三种实现
 * 1.lru：基于最近最少使用原则删除多余缓存，保持最热的 数据被缓存
 * 2.threadlocal：当前线程缓存，比如一个页面渲染，用到很多portal，每个portal都要去查用户信息，通过线程缓存，可以减少这种多余访问
 * 3.jcache：与JSR107集成，可以桥接各种缓存实现
 *
 * CacheFilter
 */
@Activate(group = {Constants.CONSUMER, Constants.PROVIDER}, value = Constants.CACHE_KEY)
public class CacheFilter implements Filter {
    /**
     * CacheFactory$Adaptive对象
     *
     * 通过Dubbo SPI机制，调用{@link #setCacheFactory(CacheFactory)}方法，进行注入
     */
    private CacheFactory cacheFactory;

    public void setCacheFactory(CacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
    }

    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        //方法开启cache功能
        if (cacheFactory != null && ConfigUtils.isNotEmpty(invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.CACHE_KEY))) {
            //基于URL+Method为维度，获得Cache对象。
            Cache cache = cacheFactory.getCache(invoker.getUrl(), invocation);
            if (cache != null) {
                //获得Cache key
                String key = StringUtils.toArgumentString(invocation.getArguments());
                //从缓存中获得结果。若存在，则创建RpcResult对象
                Object value = cache.get(key);
                if (value != null) {
                    return new RpcResult(value);
                }
                //继续执行Filter链，最终调用服务
                Result result = invoker.invoke(invocation);
                //若非移除结果，缓存结果
                if (!result.hasException()) {
                    cache.put(key, result.getValue());
                }
                return result;
            }
        }
        //继续执行Filter链，最终调用服务
        return invoker.invoke(invocation);
    }

}
