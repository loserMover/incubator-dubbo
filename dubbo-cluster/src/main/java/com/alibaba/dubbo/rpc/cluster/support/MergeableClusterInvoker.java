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
package com.alibaba.dubbo.rpc.cluster.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.RpcResult;
import com.alibaba.dubbo.rpc.cluster.Directory;
import com.alibaba.dubbo.rpc.cluster.Merger;
import com.alibaba.dubbo.rpc.cluster.merger.MergerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unchecked")
public class MergeableClusterInvoker<T> implements Invoker<T> {

    private static final Logger log = LoggerFactory.getLogger(MergeableClusterInvoker.class);
    /**
     * Directory$Adaptive对象
     */
    private final Directory<T> directory;
    /**
     * ExecutorService对象，并且为CachedThreadPool
     */
    private ExecutorService executor = Executors.newCachedThreadPool(new NamedThreadFactory("mergeable-cluster-executor", true));

    public MergeableClusterInvoker(Directory<T> directory) {
        this.directory = directory;
    }

    @SuppressWarnings("rawtypes")
    public Result invoke(final Invocation invocation) throws RpcException {
        //获得Invoker集合
        List<Invoker<T>> invokers = directory.list(invocation);
        //获得Merger扩展名
        String merger = getUrl().getMethodParameter(invocation.getMethodName(), Constants.MERGER_KEY);
        //若未配置扩展，直接调用首个可用的Invoker对象
        if (ConfigUtils.isEmpty(merger)) { // If a method doesn't have a merger, only invoke one Group
            for (final Invoker<T> invoker : invokers) {
                if (invoker.isAvailable()) {
                    //RPC调用
                    return invoker.invoke(invocation);
                }
            }
            return invokers.iterator().next().invoke(invocation);
        }
        //通过反射，获得返回类型
        Class<?> returnType;
        try {
            returnType = getInterface().getMethod(
                    invocation.getMethodName(), invocation.getParameterTypes()).getReturnType();
        } catch (NoSuchMethodException e) {
            returnType = null;
        }
        //提交线程池，并行执行，发起RPC调用，并添加到results中
        Map<String, Future<Result>> results = new HashMap<String, Future<Result>>();
        for (final Invoker<T> invoker : invokers) {
            Future<Result> future = executor.submit(new Callable<Result>() {
                public Result call() throws Exception {
                    //RPC调用
                    return invoker.invoke(new RpcInvocation(invocation, invoker));
                }
            });
            results.put(invoker.getUrl().getServiceKey(), future);
        }

        Object result = null;
        //阻塞等待执行结果，并添加到resultList中
        List<Result> resultList = new ArrayList<Result>(results.size());

        int timeout = getUrl().getMethodParameter(invocation.getMethodName(), Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
        for (Map.Entry<String, Future<Result>> entry : results.entrySet()) {
            Future<Result> future = entry.getValue();
            try {
                Result r = future.get(timeout, TimeUnit.MILLISECONDS);
                if (r.hasException()) {//异常Result，打印错误日志，忽略
                    log.error(new StringBuilder(32).append("Invoke ")
                                    .append(getGroupDescFromServiceKey(entry.getKey()))
                                    .append(" failed: ")
                                    .append(r.getException().getMessage()).toString(),
                            r.getException());
                } else {//正常Result，添加到resultList中
                    resultList.add(r);
                }
            } catch (Exception e) { //异常，抛出RpcException异常
                throw new RpcException(new StringBuilder(32)
                        .append("Failed to invoke service ")
                        .append(entry.getKey())
                        .append(": ")
                        .append(e.getMessage()).toString(),
                        e);
            }
        }
        //结果大小为空，返回空的RpcResult
        if (resultList.isEmpty()) {
            return new RpcResult((Object) null);
        //结果大小为1，返回首个RpcResult
        } else if (resultList.size() == 1) {
            return resultList.iterator().next();
        }
        //返回类型void，返回空的Result
        if (returnType == void.class) {
            return new RpcResult((Object) null);
        }
        //【第1种】基于合并方法
        if (merger.startsWith(".")) {
            //获得合并方法Method
            merger = merger.substring(1);
            Method method;
            try {
                method = returnType.getMethod(merger, returnType);
            } catch (NoSuchMethodException e) {
                throw new RpcException(new StringBuilder(32)
                        .append("Can not merge result because missing method [ ")
                        .append(merger)
                        .append(" ] in class [ ")
                        .append(returnType.getClass().getName())
                        .append(" ]")
                        .toString());
            }
            //有Method，进行合并
            if (!Modifier.isPublic(method.getModifiers())) {
                method.setAccessible(true);
            }
            result = resultList.remove(0).getValue();
            try {
                //方法返回类型匹配，合并时，修改result
                if (method.getReturnType() != void.class
                        && method.getReturnType().isAssignableFrom(result.getClass())) {
                    for (Result r : resultList) {
                        result = method.invoke(result, r.getValue());
                    }
                //方法返回类型不匹配，合并时，不修改result
                } else {
                    for (Result r : resultList) {
                        method.invoke(result, r.getValue());
                    }
                }
            } catch (Exception e) {
                throw new RpcException(
                        new StringBuilder(32)
                                .append("Can not merge result: ")
                                .append(e.getMessage()).toString(),
                        e);
            }
        //【第2种】基于Merger
        } else {
            Merger resultMerger;
            //【第2.1种】根据返回值类型自动匹配Merger
            if (ConfigUtils.isDefault(merger)) {
                resultMerger = MergerFactory.getMerger(returnType);
            //【第2.2种】指定Merger
            } else {
                resultMerger = ExtensionLoader.getExtensionLoader(Merger.class).getExtension(merger);
            }
            //有Merger，进行合并
            if (resultMerger != null) {
                List<Object> rets = new ArrayList<Object>(resultList.size());
                for (Result r : resultList) {
                    rets.add(r.getValue());
                }
                result = resultMerger.merge(
                        rets.toArray((Object[]) Array.newInstance(returnType, 0)));
            //无Merger，抛出RpcException异常
            } else {
                throw new RpcException("There is no merger to merge result.");
            }
        }
        //返回RpcResult结果
        return new RpcResult(result);
    }

    public Class<T> getInterface() {
        return directory.getInterface();
    }

    public URL getUrl() {
        return directory.getUrl();
    }

    public boolean isAvailable() {
        return directory.isAvailable();
    }

    public void destroy() {
        directory.destroy();
    }

    private String getGroupDescFromServiceKey(String key) {
        int index = key.indexOf("/");
        if (index > 0) {
            return new StringBuilder(32).append("group [ ")
                    .append(key.substring(0, index)).append(" ]").toString();
        }
        return key;
    }
}
