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

package com.alibaba.dubbo.rpc.protocol;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * AbstractProxyProtocol
 */
public abstract class AbstractProxyProtocol extends AbstractProtocol {
    /**
     * 需要抛出的异常类集合，详见{@link #refer(Class, URL)}方法
     */
    private final List<Class<?>> rpcExceptions = new CopyOnWriteArrayList<Class<?>>();
    /**
     * ProxyFactory对象
     */
    private ProxyFactory proxyFactory;

    public AbstractProxyProtocol() {
    }

    public AbstractProxyProtocol(Class<?>... exceptions) {
        for (Class<?> exception : exceptions) {
            addRpcException(exception);
        }
    }

    public void addRpcException(Class<?> exception) {
        this.rpcExceptions.add(exception);
    }

    public ProxyFactory getProxyFactory() {
        return proxyFactory;
    }

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    @SuppressWarnings("unchecked")
    public <T> Exporter<T> export(final Invoker<T> invoker) throws RpcException {
        /**
         * 获取服务键   例如：groupService/com.alibaba.dubbo.demo.DemoService:version:port
         */
        final String uri = serviceKey(invoker.getUrl());
        /**
         * 获得Exporter对象。若已经暴露，直接返回。
         */
        Exporter<T> exporter = (Exporter<T>) exporterMap.get(uri);
        if (exporter != null) {
            return exporter;
        }
        /**
         * 执行暴露服务
         */
        final Runnable runnable = doExport(proxyFactory.getProxy(invoker), invoker.getInterface(), invoker.getUrl());
        /**
         * 创建Exporter对象
         */
        exporter = new AbstractExporter<T>(invoker) {
            public void unexport() {
                //取消暴露
                super.unexport();
                exporterMap.remove(uri);
                //执行取消暴露服务的的回调
                if (runnable != null) {
                    try {
                        runnable.run();
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }
            }
        };
        //添加到exporterMap集合
        exporterMap.put(uri, exporter);
        return exporter;
    }

    /**
     *  引用远程服务
     * @param type 服务的类型
     * @param url  远程服务的URL地址
     * @param <T>
     * @return
     * @throws RpcException
     */
    public <T> Invoker<T> refer(final Class<T> type, final URL url) throws RpcException {
        // 执行引用服务
        final Invoker<T> tagert = proxyFactory.getInvoker(doRefer(type, url), type, url);
        //创建Invoker对象
        Invoker<T> invoker = new AbstractInvoker<T>(type, url) {
            @Override
            protected Result doInvoke(Invocation invocation) throws Throwable {
                try {
                    //调用 RPC服务，返回结果
                    Result result = tagert.invoke(invocation);
                    //若返回结果带有异常，并且需要抛出，则抛出异常
                    Throwable e = result.getException();
                    if (e != null) {
                        for (Class<?> rpcException : rpcExceptions) {
                            if (rpcException.isAssignableFrom(e.getClass())) {
                                throw getRpcException(type, url, invocation, e);
                            }
                        }
                    }
                    return result;
                } catch (RpcException e) {
                    //若是未知异常，则获得对应的错误码
                    if (e.getCode() == RpcException.UNKNOWN_EXCEPTION) {
                        e.setCode(getErrorCode(e.getCause()));
                    }
                    throw e;
                } catch (Throwable e) {
                    //抛出RpcException异常
                    throw getRpcException(type, url, invocation, e);
                }
            }
        };
        //添加到invokers集合
        invokers.add(invoker);
        return invoker;
    }

    /**
     * 包装成RpcException异常
     * @param type service
     * @param url URL
     * @param invocation Invocation
     * @param e Throwable
     * @return
     */
    protected RpcException getRpcException(Class<?> type, URL url, Invocation invocation, Throwable e) {
        RpcException re = new RpcException("Failed to invoke remote service: " + type + ", method: "
                + invocation.getMethodName() + ", cause: " + e.getMessage(), e);
        re.setCode(getErrorCode(e));
        return re;
    }

    /**
     * 获取服务器地址（IP:Port）
     * @param url
     * @return
     */
    protected String getAddr(URL url) {
        String bindIp = url.getParameter(Constants.BIND_IP_KEY, url.getHost());
        if (url.getParameter(Constants.ANYHOST_KEY, false)) {
            bindIp = Constants.ANYHOST_VALUE;
        }
        return NetUtils.getIpByHost(bindIp) + ":" + url.getParameter(Constants.BIND_PORT_KEY, url.getPort());
    }
    /**
     * 获得异常对应的错误码
     * @param e
     * @return
     */
    protected int getErrorCode(Throwable e) {
        return RpcException.UNKNOWN_EXCEPTION;
    }

    /**
     * 执行服务暴露，并返回取消暴露的回调Runable
     * @param impl 服务Proxy对象
     * @param type 服务接口
     * @param url URL
     * @param <T> 服务接口
     * @return 取消暴露的回调Runable
     * @throws RpcException 异常
     */
    protected abstract <T> Runnable doExport(T impl, Class<T> type, URL url) throws RpcException;

    /**
     * 执行引用，并返回调用远程服务的Service对象
     * @param type 服务接口
     * @param url URL
     * @param <T> 服务接口
     * @return 调用远程服务的
     * @throws RpcException
     */
    protected abstract <T> T doRefer(Class<T> type, URL url) throws RpcException;

}
