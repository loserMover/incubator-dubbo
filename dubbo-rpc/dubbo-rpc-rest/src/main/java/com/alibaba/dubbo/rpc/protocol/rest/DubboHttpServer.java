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
package com.alibaba.dubbo.rpc.protocol.rest;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.http.HttpBinder;
import com.alibaba.dubbo.remoting.http.HttpHandler;
import com.alibaba.dubbo.remoting.http.HttpServer;
import com.alibaba.dubbo.remoting.http.servlet.BootstrapListener;
import com.alibaba.dubbo.remoting.http.servlet.ServletManager;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;

import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.spi.ResteasyDeployment;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Enumeration;

public class DubboHttpServer extends BaseRestServer {
    /**
     * Resteasy HttpServletDispatcher
     */
    private final HttpServletDispatcher dispatcher = new HttpServletDispatcher();
    /**
     * Resteasy ResteasyDeployment
     */
    private final ResteasyDeployment deployment = new ResteasyDeployment();
    /**
     * Dubbo HttpBinder$Adaptive
     */
    private HttpBinder httpBinder;
    /**
     * HttpServer对象
     */
    private HttpServer httpServer;
//    private boolean isExternalServer;

    public DubboHttpServer(HttpBinder httpBinder) {
        this.httpBinder = httpBinder;
    }

    protected void doStart(URL url) {
        //创建HttpServer对象，使用RestHandler作为处理器
        // TODO jetty will by default enable keepAlive so the xml config has no effect now
        httpServer = httpBinder.bind(url, new RestHandler());
        //获得ServletContext对象
        ServletContext servletContext = ServletManager.getInstance().getServletContext(url.getPort());
        if (servletContext == null) {
            servletContext = ServletManager.getInstance().getServletContext(ServletManager.EXTERNAL_SERVER_PORT);
        }
        if (servletContext == null) {
            throw new RpcException("No servlet context found. If you are using server='servlet', " +
                    "make sure that you've configured " + BootstrapListener.class.getName() + " in web.xml");
        }
        //设置ResteasyDeployment
        servletContext.setAttribute(ResteasyDeployment.class.getName(), deployment); // https://github.com/resteasy/Resteasy/blob/master/server-adapters/resteasy-undertow/src/main/java/org/jboss/resteasy/plugins/server/undertow/UndertowJaxrsServer.java#L74
        //初始化Reasteasy HttpServletDispatcher
        try {
            dispatcher.init(new SimpleServletConfig(servletContext));
        } catch (ServletException e) {
            throw new RpcException(e);
        }
    }

    public void stop() {
        httpServer.close();
    }

    protected ResteasyDeployment getDeployment() {
        return deployment;
    }

    private class RestHandler implements HttpHandler {

        public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            //设置调用地址和端口
            RpcContext.getContext().setRemoteAddress(request.getRemoteAddr(), request.getRemotePort());
            //调读请求
            dispatcher.service(request, response);
        }
    }

    private static class SimpleServletConfig implements ServletConfig {

        private final ServletContext servletContext;

        public SimpleServletConfig(ServletContext servletContext) {
            this.servletContext = servletContext;
        }

        public String getServletName() {
            return "DispatcherServlet";
        }

        public ServletContext getServletContext() {
            return servletContext;
        }

        public String getInitParameter(String s) {
            return null;
        }

        public Enumeration getInitParameterNames() {
            return new Enumeration() {
                public boolean hasMoreElements() {
                    return false;
                }

                public Object nextElement() {
                    return null;
                }
            };
        }
    }
}
