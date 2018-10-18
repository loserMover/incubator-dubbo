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
package com.alibaba.dubbo.rpc.proxy;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.service.EchoService;

/**
 * AbstractProxyFactory
 * @desc 实现了 #getProxy(invoker) 方法，获得需要生成代理的接口们。
 */
public abstract class AbstractProxyFactory implements ProxyFactory {

    public <T> T getProxy(Invoker<T> invoker) throws RpcException {
        Class<?>[] interfaces = null;
        //获得所有接口，同时Invoker和EchoService
        String config = invoker.getUrl().getParameter("interfaces");
        if (config != null && config.length() > 0) {
            String[] types = Constants.COMMA_SPLIT_PATTERN.split(config);
            if (types != null && types.length > 0) {
                interfaces = new Class<?>[types.length + 2];
                interfaces[0] = invoker.getInterface();
                interfaces[1] = EchoService.class;
                for (int i = 0; i < types.length; i++) {
                    interfaces[i + 1] = ReflectUtils.forName(types[i]);
                }
            }
        }
        // 增加 EchoService 接口，用于回生测试。参见文档《回声测试》https://dubbo.gitbooks.io/dubbo-user-book/demos/echo-service.html
        if (interfaces == null) {
            interfaces = new Class<?>[]{invoker.getInterface(), EchoService.class};
        }

        return getProxy(invoker, interfaces);
    }

    /**
     * @desc 获得Proxy对象
     * @param invoker Invoker
     * @param types 代理接口数组
     * @param <T> 泛型
     * @return 代理对象
     */
    public abstract <T> T getProxy(Invoker<T> invoker, Class<?>[] types);

}