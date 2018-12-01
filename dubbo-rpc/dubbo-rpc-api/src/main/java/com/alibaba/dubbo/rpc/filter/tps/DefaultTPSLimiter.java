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
package com.alibaba.dubbo.rpc.filter.tps;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 实现TPSLimiter接口，默认TPS限制器实现类，以服务为维度
 */
public class DefaultTPSLimiter implements TPSLimiter {
    /**
     * StatItem集合
     *
     * key：服务名
     */
    private final ConcurrentMap<String, StatItem> stats
            = new ConcurrentHashMap<String, StatItem>();

    public boolean isAllowable(URL url, Invocation invocation) {
        //获得TPS大小配置项
        int rate = url.getParameter(Constants.TPS_LIMIT_RATE_KEY, -1);
        //获得TPS周期配置项（单位：毫秒），默认60秒
        long interval = url.getParameter(Constants.TPS_LIMIT_INTERVAL_KEY,
                Constants.DEFAULT_TPS_LIMIT_INTERVAL);
        //获得服务键
        String serviceKey = url.getServiceKey();
        //限流
        if (rate > 0) {
            //获得StatItem对象
            StatItem statItem = stats.get(serviceKey);
            //若不存在，则进行创建
            if (statItem == null) {
                stats.putIfAbsent(serviceKey,
                        new StatItem(serviceKey, rate, interval));
                statItem = stats.get(serviceKey);
            }
            //根据TPS限流规则判断是否限制此次调用
            return statItem.isAllowable();
        } else {
            //移除StatItem
            StatItem statItem = stats.get(serviceKey);
            if (statItem != null) {
                stats.remove(serviceKey);
            }
        }
        //返回通过
        return true;
    }

}
