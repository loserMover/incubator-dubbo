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
package com.alibaba.dubbo.rpc.protocol.injvm;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.protocol.AbstractProtocol;
import com.alibaba.dubbo.rpc.support.ProtocolUtils;

import java.util.Map;

/**
 * InjvmProtocol
 */
public class InjvmProtocol extends AbstractProtocol implements Protocol {
    /**
     * @desc 协议的名
     */
    public static final String NAME = Constants.LOCAL_PROTOCOL;
    /**
     * @desc 默认端口
     */
    public static final int DEFAULT_PORT = 0;
    /**
     * @desc 单例，在dubbo SPI中，被初始化，有且仅有一次
     */
    private static InjvmProtocol INSTANCE;

    public InjvmProtocol() {
        INSTANCE = this;
    }

    public static InjvmProtocol getInjvmProtocol() {
        if (INSTANCE == null) {
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(InjvmProtocol.NAME); // load
        }
        return INSTANCE;
    }

    /**
     * @desc 获得Exporter对象
     * @param map Exporter对象集合
     * @param key URL
     * @return Exporter
     */
    static Exporter<?> getExporter(Map<String, Exporter<?>> map, URL key) {
        Exporter<?> result = null;
        //全匹配
        if (!key.getServiceKey().contains("*")) {
            result = map.get(key.getServiceKey());
        } else {// 带 * 时，循环匹配，依然匹配的是 `group` `version` `interface` 属性。带 * 的原因是，version = * ，所有版本
            if (map != null && !map.isEmpty()) {
                for (Exporter<?> exporter : map.values()) {
                    if (UrlUtils.isServiceKeyMatch(key, exporter.getInvoker().getUrl())) {
                        result = exporter;
                        break;
                    }
                }
            }
        }

        if (result == null) {
            return null;
        } else if (ProtocolUtils.isGeneric( //泛化调用
                result.getInvoker().getUrl().getParameter(Constants.GENERIC_KEY))) {
            return null;
        } else {
            return result;
        }
    }

    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    /**
     *
     * @param invoker 服务的执行体
     * @param <T>
     * @return
     * @throws RpcException
     */
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        //创建 InjvmExporter 对象
        return new InjvmExporter<T>(invoker, invoker.getUrl().getServiceKey(), exporterMap);
    }

    public <T> Invoker<T> refer(Class<T> serviceType, URL url) throws RpcException {
        //创建InjvmInvoker对象
        return new InjvmInvoker<T>(serviceType, url, url.getServiceKey(), exporterMap);
    }

    /**
     * @desc 通过URL参数判断是否为本地引用
     * @param url
     * @return
     */
    public boolean isInjvmRefer(URL url) {
        final boolean isJvmRefer;
        String scope = url.getParameter(Constants.SCOPE_KEY);
        // Since injvm protocol is configured explicitly, we don't need to set any extra flag, use normal refer process.
        //当‘protocol=injvm’时，本身已经是jvm协议了，走正常流程
        if (Constants.LOCAL_PROTOCOL.toString().equals(url.getProtocol())) { //改方法调用的地方在{@link ReferenceConfig#createProxy()},url传入的协议protocol=temp，所以根本不回走此流程
            isJvmRefer = false;
        } else if (Constants.SCOPE_LOCAL.equals(scope) || (url.getParameter("injvm", false))) {
            // if it's declared as local reference
            // 'scope=local' is equivalent to 'injvm=true', injvm will be deprecated in the future release
            // 当 `scope = local` 或者 `injvm = true` 时，本地引用
            isJvmRefer = true;
        } else if (Constants.SCOPE_REMOTE.equals(scope)) {
            // it's declared as remote reference
            // 当 `scope = remote` 时，远程引用
            isJvmRefer = false;
        } else if (url.getParameter(Constants.GENERIC_KEY, false)) {
            // generic invocation is not local reference
            // 当 `generic = true` 时，即使用泛化调用，远程引用
            isJvmRefer = false;
        } else if (getExporter(exporterMap, url) != null) {
            // by default, go through local reference if there's the service exposed locally
            // 当本地已经有该 Exporter 时，本地引用
            isJvmRefer = true;
        } else {
            // 默认，远程引用
            isJvmRefer = false;
        }
        return isJvmRefer;
    }
}