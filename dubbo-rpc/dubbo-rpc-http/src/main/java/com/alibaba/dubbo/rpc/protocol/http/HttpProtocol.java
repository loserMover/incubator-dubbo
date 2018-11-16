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
package com.alibaba.dubbo.rpc.protocol.http;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.http.HttpBinder;
import com.alibaba.dubbo.remoting.http.HttpHandler;
import com.alibaba.dubbo.remoting.http.HttpServer;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.protocol.AbstractProxyProtocol;

import org.springframework.remoting.RemoteAccessException;
import org.springframework.remoting.httpinvoker.HttpComponentsHttpInvokerRequestExecutor;
import org.springframework.remoting.httpinvoker.HttpInvokerProxyFactoryBean;
import org.springframework.remoting.httpinvoker.HttpInvokerServiceExporter;
import org.springframework.remoting.httpinvoker.SimpleHttpInvokerRequestExecutor;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HttpProtocol
 */
public class HttpProtocol extends AbstractProxyProtocol {
    /**
     * 默认服务端口
     */
    public static final int DEFAULT_PORT = 80;
    /**
     * Http 服务器集合
     *
     * key：ip:port
     */
    private final Map<String, HttpServer> serverMap = new ConcurrentHashMap<String, HttpServer>();
    /**
     * spring HttpInvokerServiceExporter集合
     *
     * 请求处理过程为 HttpServer => DispatcherServlet => InternalHandler => HttpInvokerServiceExporter
     *
     * key：path 服务名
     */
    private final Map<String, HttpInvokerServiceExporter> skeletonMap = new ConcurrentHashMap<String, HttpInvokerServiceExporter>();
    /**
     * HttpBinder$Adaptive对象
     */
    private HttpBinder httpBinder;

    public HttpProtocol() {
        super(RemoteAccessException.class);
    }

    public void setHttpBinder(HttpBinder httpBinder) {
        this.httpBinder = httpBinder;
    }

    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    protected <T> Runnable doExport(final T impl, Class<T> type, URL url) throws RpcException {
        //获取服务器地址
        String addr = getAddr(url);
        //获得HttpServer对象，若不存在，则进行穿件
        HttpServer server = serverMap.get(addr);
        if (server == null) {
            server = httpBinder.bind(url, new InternalHandler());//InernalHandler
            serverMap.put(addr, server);
        }
        //创建HttpInvokerServiceExporter对象
        final HttpInvokerServiceExporter httpServiceExporter = new HttpInvokerServiceExporter();
        httpServiceExporter.setServiceInterface(type);
        httpServiceExporter.setService(impl);
        try {
            httpServiceExporter.afterPropertiesSet();
        } catch (Exception e) {
            throw new RpcException(e.getMessage(), e);
        }
        //添加到skeletonMap集合中
        final String path = url.getAbsolutePath();
        skeletonMap.put(path, httpServiceExporter);
        //返回取消暴露回调Runnable
        return new Runnable() {
            public void run() {
                skeletonMap.remove(path);
            }
        };
    }

    /**
     * 执行引用，并返回调用远程服务的Service对象
     * @param serviceType 服务接口
     * @param url URL
     * @param <T> 服务接口
     * @return 调用远程服务的
     * @throws RpcException
     */
    @SuppressWarnings("unchecked")
    protected <T> T doRefer(final Class<T> serviceType, final URL url) throws RpcException {
        //创建HttpInvokerProxyFactoryBean 对象
        final HttpInvokerProxyFactoryBean httpProxyFactoryBean = new HttpInvokerProxyFactoryBean();
        httpProxyFactoryBean.setServiceUrl(url.toIdentityString());
        httpProxyFactoryBean.setServiceInterface(serviceType);
        //创建执行器SimpleHttpInvokerRequestExecutor对象（使用JDK HttpClient）
        String client = url.getParameter(Constants.CLIENT_KEY);
        if (client == null || client.length() == 0 || "simple".equals(client)) {
            SimpleHttpInvokerRequestExecutor httpInvokerRequestExecutor = new SimpleHttpInvokerRequestExecutor() {
                protected void prepareConnection(HttpURLConnection con,
                                                 int contentLength) throws IOException {
                    super.prepareConnection(con, contentLength);
                    con.setReadTimeout(url.getParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT));
                    con.setConnectTimeout(url.getParameter(Constants.CONNECT_TIMEOUT_KEY, Constants.DEFAULT_CONNECT_TIMEOUT));
                }
            };
            httpProxyFactoryBean.setHttpInvokerRequestExecutor(httpInvokerRequestExecutor);
        } else if ("commons".equals(client)) {
            // 创建执行器 HttpComponentsHttpInvokerRequestExecutor 对象 (使用Apache HttpClient)
            HttpComponentsHttpInvokerRequestExecutor httpInvokerRequestExecutor = new HttpComponentsHttpInvokerRequestExecutor();
            httpInvokerRequestExecutor.setReadTimeout(url.getParameter(Constants.CONNECT_TIMEOUT_KEY, Constants.DEFAULT_CONNECT_TIMEOUT));
            httpProxyFactoryBean.setHttpInvokerRequestExecutor(httpInvokerRequestExecutor);
        } else {
            throw new IllegalStateException("Unsupported http protocol client " + client + ", only supported: simple, commons");
        }
        httpProxyFactoryBean.afterPropertiesSet();
        // 返回 HttpInvokerProxyFactoryBean 对象
        return (T) httpProxyFactoryBean.getObject();
    }

    /**
     * 获得异常对应的错误码
     * @param e
     * @return
     */
    protected int getErrorCode(Throwable e) {
        if (e instanceof RemoteAccessException) {
            e = e.getCause();
        }
        if (e != null) {
            Class<?> cls = e.getClass();
            if (SocketTimeoutException.class.equals(cls)) {
                return RpcException.TIMEOUT_EXCEPTION;
            } else if (IOException.class.isAssignableFrom(cls)) {
                return RpcException.NETWORK_EXCEPTION;
            } else if (ClassNotFoundException.class.isAssignableFrom(cls)) {
                return RpcException.SERIALIZATION_EXCEPTION;
            }
        }
        return super.getErrorCode(e);
    }

    private class InternalHandler implements HttpHandler {

        public void handle(HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {
            String uri = request.getRequestURI();
            //获得HttpInvokerServiceExporter对象
            HttpInvokerServiceExporter skeleton = skeletonMap.get(uri);
            //必须是POST请求
            if (!request.getMethod().equalsIgnoreCase("POST")) {
                response.setStatus(500);
            } else {
                //执行调用
                RpcContext.getContext().setRemoteAddress(request.getRemoteAddr(), request.getRemotePort());
                try {
                    skeleton.handleRequest(request, response);
                } catch (Throwable e) {
                    throw new ServletException(e);
                }
            }
        }

    }

}