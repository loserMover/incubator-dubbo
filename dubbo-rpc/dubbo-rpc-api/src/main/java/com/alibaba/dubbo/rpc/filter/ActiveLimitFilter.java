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
package com.alibaba.dubbo.rpc.filter;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcStatus;

/**
 * 服务消费者每服务、每方法的最大可并行调用数限制的过滤器实现类。
 * LimitInvokerFilter
 */
@Activate(group = Constants.CONSUMER, value = Constants.ACTIVES_KEY)
public class ActiveLimitFilter implements Filter {

    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        //获得URL对象
        URL url = invoker.getUrl();
        //获得方法名
        String methodName = invocation.getMethodName();
        // 获得服务提供者每服务每方法最大可并行执行请求数
        int max = invoker.getUrl().getMethodParameter(methodName, Constants.ACTIVES_KEY, 0);
        // 获得 RpcStatus 对象，基于服务 URL + 方法维度
        RpcStatus count = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName());
        if (max > 0) {
            // 获得超时值
            long timeout = invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.TIMEOUT_KEY, 0);
            long start = System.currentTimeMillis();
            long remain = timeout; // 剩余可等待时间
            int active = count.getActive();
            // 超过最大可并行执行请求数，继续等待
            if (active >= max) {
                synchronized (count) { // 通过锁，有且仅有一个在等待。
                    while ((active = count.getActive()) >= max) {
                        try {
                            // 等待，直到超时，或者被唤醒
                            count.wait(remain);
                        } catch (InterruptedException e) {
                        }
                        // 判断是否没有剩余时长了，抛出 RpcException 异常
                        long elapsed = System.currentTimeMillis() - start;
                        remain = timeout - elapsed;
                        if (remain <= 0) {
                            throw new RpcException("Waiting concurrent invoke timeout in client-side for service:  "
                                    + invoker.getInterface().getName() + ", method: "
                                    + invocation.getMethodName() + ", elapsed: " + elapsed
                                    + ", timeout: " + timeout + ". concurrent invokes: " + active
                                    + ". max concurrent invoke limit: " + max);
                        }
                    }
                }
            }
        }
        try {
            //调用开始计时
            long begin = System.currentTimeMillis();
            RpcStatus.beginCount(url, methodName);
            try {
                //执行Filter链，最后执行服务
                Result result = invoker.invoke(invocation);
                // 调用结束的计数（成功）
                RpcStatus.endCount(url, methodName, System.currentTimeMillis() - begin, true);
                return result;
            } catch (RuntimeException t) {
                // 调用结束的计数（失败）
                RpcStatus.endCount(url, methodName, System.currentTimeMillis() - begin, false);
                throw t;
            }
        } finally {
            // 唤醒等待的相同服务的相同方法的请求
            if (max > 0) {
                synchronized (count) {
                    count.notify();
                }
            }
        }
    }

}