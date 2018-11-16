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
package com.alibaba.dubbo.rpc.protocol.hessian;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.http.HttpBinder;
import com.alibaba.dubbo.remoting.http.HttpHandler;
import com.alibaba.dubbo.remoting.http.HttpServer;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.protocol.AbstractProxyProtocol;

import com.caucho.hessian.HessianException;
import com.caucho.hessian.client.HessianConnectionException;
import com.caucho.hessian.client.HessianProxyFactory;
import com.caucho.hessian.io.HessianMethodSerializationException;
import com.caucho.hessian.server.HessianSkeleton;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * http rpc support.
 */
public class HessianProtocol extends AbstractProxyProtocol {
    /**
     * Http 服务器集合
     */
    private final Map<String, HttpServer> serverMap = new ConcurrentHashMap<String, HttpServer>();
    /**
     * Hessian HessianSkeleton集合
     *
     * HttpServer => DispatcherServlet => HessianHandler => HessianSkeleton
     *
     * key： path 服务名
     */
    private final Map<String, HessianSkeleton> skeletonMap = new ConcurrentHashMap<String, HessianSkeleton>();
    /**
     * HttpBinder$Adaptive对象
     */
    private HttpBinder httpBinder;

    public HessianProtocol() {
        super(HessianException.class);
    }

    public void setHttpBinder(HttpBinder httpBinder) {
        this.httpBinder = httpBinder;
    }

    public int getDefaultPort() {
        return 80;
    }

    protected <T> Runnable doExport(T impl, Class<T> type, URL url) throws RpcException {
        //获得服务器地址
        String addr = getAddr(url);
        //获得HttpServer对象，若不存在，则进行创建
        HttpServer server = serverMap.get(addr);
        if (server == null) {
            server = httpBinder.bind(url, new HessianHandler());
            serverMap.put(addr, server);
        }
        //添加到skeletonMap集合中
        final String path = url.getAbsolutePath();
        HessianSkeleton skeleton = new HessianSkeleton(impl, type);
        skeletonMap.put(path, skeleton);
        //返回取消暴露的回调Runnable对象
        return new Runnable() {
            public void run() {
                skeletonMap.remove(path);
            }
        };
    }

    @SuppressWarnings("unchecked")
    protected <T> T doRefer(Class<T> serviceType, URL url) throws RpcException {
        //创建HessianProxyFactory对象
        HessianProxyFactory hessianProxyFactory = new HessianProxyFactory();
        //是否Hessian到请求
        boolean isHessian2Request = url.getParameter(Constants.HESSIAN2_REQUEST_KEY, Constants.DEFAULT_HESSIAN2_REQUEST);
        hessianProxyFactory.setHessian2Request(isHessian2Request);
        //是否重新覆盖方法
        boolean isOverloadEnabled = url.getParameter(Constants.HESSIAN_OVERLOAD_METHOD_KEY, Constants.DEFAULT_HESSIAN_OVERLOAD_METHOD);
        hessianProxyFactory.setOverloadEnabled(isOverloadEnabled);
        //创建连接器工厂为HttpClientConnectionFactory对象，即Apache HttpClient
        String client = url.getParameter(Constants.CLIENT_KEY, Constants.DEFAULT_HTTP_CLIENT);
        if ("httpclient".equals(client)) {
            hessianProxyFactory.setConnectionFactory(new HttpClientConnectionFactory());
        } else if (client != null && client.length() > 0 && !Constants.DEFAULT_HTTP_CLIENT.equals(client)) {
            throw new IllegalStateException("Unsupported http protocol client=\"" + client + "\"!");
        }
        //设置超时时间
        int timeout = url.getParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
        hessianProxyFactory.setConnectTimeout(timeout);
        hessianProxyFactory.setReadTimeout(timeout);
        //创建ServiceProxy对象
        return (T) hessianProxyFactory.create(serviceType, url.setProtocol("http").toJavaURL(), Thread.currentThread().getContextClassLoader());
    }

    protected int getErrorCode(Throwable e) {
        if (e instanceof HessianConnectionException) {
            if (e.getCause() != null) {
                Class<?> cls = e.getCause().getClass();
                if (SocketTimeoutException.class.equals(cls)) {
                    return RpcException.TIMEOUT_EXCEPTION;
                }
            }
            return RpcException.NETWORK_EXCEPTION;
        } else if (e instanceof HessianMethodSerializationException) {
            return RpcException.SERIALIZATION_EXCEPTION;
        }
        return super.getErrorCode(e);
    }

    /**
     * 销毁
     */
    public void destroy() {
        //执行父类销毁
        super.destroy();
        //销毁所有HttpServer
        for (String key : new ArrayList<String>(serverMap.keySet())) {
            HttpServer server = serverMap.remove(key);
            if (server != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Close hessian server " + server.getUrl());
                    }
                    server.close();
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        }
    }

    private class HessianHandler implements HttpHandler {

        public void handle(HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {
            String uri = request.getRequestURI();
            //获得HessianSkeleton对象
            //必须是POST请求
            HessianSkeleton skeleton = skeletonMap.get(uri);
            if (!request.getMethod().equalsIgnoreCase("POST")) {
                response.setStatus(500);
            } else {
                //执行调用
                RpcContext.getContext().setRemoteAddress(request.getRemoteAddr(), request.getRemotePort());
                try {
                    skeleton.invoke(request.getInputStream(), response.getOutputStream());
                } catch (Throwable e) {
                    throw new ServletException(e);
                }
            }
        }

    }

}
