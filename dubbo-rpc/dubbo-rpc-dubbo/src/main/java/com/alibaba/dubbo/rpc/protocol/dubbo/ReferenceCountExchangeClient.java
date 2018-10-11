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
package com.alibaba.dubbo.rpc.protocol.dubbo;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.Parameters;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.exchange.ExchangeClient;
import com.alibaba.dubbo.remoting.exchange.ExchangeHandler;
import com.alibaba.dubbo.remoting.exchange.ResponseFuture;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * dubbo protocol support class.
 */
@SuppressWarnings("deprecation")
final class ReferenceCountExchangeClient implements ExchangeClient {
    /**
     * @desc URL
     */
    private final URL url;
    /**
     * @desc 指向数量
     */
    private final AtomicInteger refenceCount = new AtomicInteger(0);
    /**
     * @desc 幽灵客户端集合,使用的是{@link DubboProtocol#ghostClientMap}的幽灵集合
     */
    //    private final ExchangeHandler handler;
    private final ConcurrentMap<String, LazyConnectExchangeClient> ghostClientMap;
    /**
     * @desc 客户端集合
     * 【创建】构造方法，传入 client 属性，指向它。
     * 【关闭】关闭方法，创建 LazyConnectExchangeClient 对象，指向该幽灵客户端。
     */
    private ExchangeClient client;


    public ReferenceCountExchangeClient(ExchangeClient client, ConcurrentMap<String, LazyConnectExchangeClient> ghostClientMap) {
        this.client = client;
        //指向加一
        refenceCount.incrementAndGet();
        this.url = client.getUrl();
        if (ghostClientMap == null) {
            throw new IllegalStateException("ghostClientMap can not be null, url: " + url);
        }
        this.ghostClientMap = ghostClientMap;
    }

    public void reset(URL url) {
        client.reset(url);
    }

    public ResponseFuture request(Object request) throws RemotingException {
        return client.request(request);
    }

    public URL getUrl() {
        return client.getUrl();
    }

    public InetSocketAddress getRemoteAddress() {
        return client.getRemoteAddress();
    }

    public ChannelHandler getChannelHandler() {
        return client.getChannelHandler();
    }

    public ResponseFuture request(Object request, int timeout) throws RemotingException {
        return client.request(request, timeout);
    }

    public boolean isConnected() {
        return client.isConnected();
    }

    public void reconnect() throws RemotingException {
        client.reconnect();
    }

    public InetSocketAddress getLocalAddress() {
        return client.getLocalAddress();
    }

    public boolean hasAttribute(String key) {
        return client.hasAttribute(key);
    }

    public void reset(Parameters parameters) {
        client.reset(parameters);
    }

    /**
     * @desc 基于装饰器模式
     * @param message
     * @throws RemotingException
     */
    public void send(Object message) throws RemotingException {
        client.send(message);
    }

    public ExchangeHandler getExchangeHandler() {
        return client.getExchangeHandler();
    }

    public Object getAttribute(String key) {
        return client.getAttribute(key);
    }

    public void send(Object message, boolean sent) throws RemotingException {
        client.send(message, sent);
    }

    public void setAttribute(String key, Object value) {
        client.setAttribute(key, value);
    }

    public void removeAttribute(String key) {
        client.removeAttribute(key);
    }

    /**
     * close() is not idempotent any longer
     */
    public void close() {
        close(0);
    }

    /**
     * @desc 关闭client
     * @param timeout
     */
    public void close(int timeout) {
        if (refenceCount.decrementAndGet() <= 0) {
            //立即关闭
            if (timeout == 0) {
                client.close();
            } else {//延迟关闭
                client.close(timeout);
            }
            //替换‘client’为LazyConnectExchangeClient对象
            client = replaceWithLazyClient();
        }
    }

    public void startClose() {
        client.startClose();
    }

    /**
     * @desc 替换‘client’为LazyConnectExchangeClient对象
     * @return
     */
    // ghost client
    private LazyConnectExchangeClient replaceWithLazyClient() {
        //基于 url ，创建 LazyConnectExchangeClient 的 URL 链接。设置的一些参数
        // this is a defensive operation to avoid client is closed by accident, the initial state of the client is false
        URL lazyUrl = url.addParameter(Constants.LAZY_CONNECT_INITIAL_STATE_KEY, Boolean.FALSE)
                .addParameter(Constants.RECONNECT_KEY, Boolean.FALSE)
                .addParameter(Constants.SEND_RECONNECT_KEY, Boolean.TRUE.toString())
                .addParameter("warning", Boolean.TRUE.toString())
                .addParameter(LazyConnectExchangeClient.REQUEST_WITH_WARNING_KEY, true)
                .addParameter("_client_memo", "referencecounthandler.replacewithlazyclient");

        String key = url.getAddress();
        // 若不存在，则新创建 LazyConnectExchangeClient 对象，
        // in worst case there's only one ghost connection.
        LazyConnectExchangeClient gclient = ghostClientMap.get(key);
        if (gclient == null || gclient.isClosed()) {
            gclient = new LazyConnectExchangeClient(lazyUrl, client.getExchangeHandler());
            ghostClientMap.put(key, gclient);
        }
        return gclient;
    }

    public boolean isClosed() {
        return client.isClosed();
    }

    /**
     * @desc 计数
     */
    public void incrementAndGetCount() {
        refenceCount.incrementAndGet();
    }
}