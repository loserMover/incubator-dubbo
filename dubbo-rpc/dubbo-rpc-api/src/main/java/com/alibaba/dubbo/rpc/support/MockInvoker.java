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
package com.alibaba.dubbo.rpc.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.PojoUtils;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.RpcResult;
import com.alibaba.fastjson.JSON;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final public class MockInvoker<T> implements Invoker<T> {
    /**
     * ProxyFactory$Adaptive
     */
    private final static ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
    /**
     * mock 与 Invoker对象的映射缓存
     *
     * @see #getInvoker(String)
     */
    private final static Map<String, Invoker<?>> mocks = new ConcurrentHashMap<String, Invoker<?>>();
    /**
     * mock 与 Throwable 对象的映射缓存
     *
     * @see #getThrowable(String)
     */
    private final static Map<String, Throwable> throwables = new ConcurrentHashMap<String, Throwable>();
    /**
     * URL对象
     */
    private final URL url;

    public MockInvoker(URL url) {
        this.url = url;
    }

    public static Object parseMockValue(String mock) throws Exception {
        return parseMockValue(mock, null);
    }

    public static Object parseMockValue(String mock, Type[] returnTypes) throws Exception {
        //解析值（不考虑返回类型）
        Object value = null;
        if ("empty".equals(mock)) {//未赋值的对象，即new XXX()对象
            value = ReflectUtils.getEmptyObject(returnTypes != null && returnTypes.length > 0 ? (Class<?>) returnTypes[0] : null);
        } else if ("null".equals(mock)) {//null
            value = null;
        } else if ("true".equals(mock)) {//true
            value = true;
        } else if ("false".equals(mock)) {//false
            value = false;
        } else if (mock.length() >= 2 && (mock.startsWith("\"") && mock.endsWith("\"")
                || mock.startsWith("\'") && mock.endsWith("\'"))) {//使用''或""的字符串，截取掉头尾
            value = mock.subSequence(1, mock.length() - 1);
        } else if (returnTypes != null && returnTypes.length > 0 && returnTypes[0] == String.class) {//字符串
            value = mock;
        } else if (StringUtils.isNumeric(mock)) {//数字
            value = JSON.parse(mock);
        } else if (mock.startsWith("{")) {//Map
            value = JSON.parseObject(mock, Map.class);
        } else if (mock.startsWith("[")) {//List
            value = JSON.parseObject(mock, List.class);
        } else {
            value = mock;
        }
        // 转换成对应的返回类型
        if (returnTypes != null && returnTypes.length > 0) {
            value = PojoUtils.realize(value, (Class<?>) returnTypes[0], returnTypes.length > 1 ? returnTypes[1] : null);
        }
        return value;
    }

    public Result invoke(Invocation invocation) throws RpcException {
        //获得'mock'配置项，方法级 > 类级
        String mock = getUrl().getParameter(invocation.getMethodName() + "." + Constants.MOCK_KEY);
        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation) invocation).setInvoker(this);
        }
        if (StringUtils.isBlank(mock)) { //不允许为空
            mock = getUrl().getParameter(Constants.MOCK_KEY);
        }

        if (StringUtils.isBlank(mock)) {
            throw new RpcException(new IllegalAccessException("mock can not be null. url :" + url));
        }
        //标准化'mock'配置项
        mock = normallizeMock(URL.decode(mock));
        //等于'return'，返回值为空的RpcResult对象
        if (Constants.RETURN_PREFIX.trim().equalsIgnoreCase(mock.trim())) {
            RpcResult result = new RpcResult();
            result.setValue(null);
            return result;
        //以'return'开头，返回对应值的RpcResult对象
        } else if (mock.startsWith(Constants.RETURN_PREFIX)) {
            mock = mock.substring(Constants.RETURN_PREFIX.length()).trim();
            mock = mock.replace('`', '"');
            try {
                //解析返回类型
                Type[] returnTypes = RpcUtils.getReturnTypes(invocation);
                //解析返回值
                Object value = parseMockValue(mock, returnTypes);
                //创建对相应值得RpcResult对象，并返回
                return new RpcResult(value);
            } catch (Exception ew) {
                throw new RpcException("mock return invoke error. method :" + invocation.getMethodName() + ", mock:" + mock + ", url: " + url, ew);
            }
        //以'throw'开头，抛出RpcException异常
        } else if (mock.startsWith(Constants.THROW_PREFIX)) {
            mock = mock.substring(Constants.THROW_PREFIX.length()).trim();
            mock = mock.replace('`', '"');
            if (StringUtils.isBlank(mock)) {
                throw new RpcException(" mocked exception for Service degradation. ");
            } else { // user customized class
                //创建自定义异常
                Throwable t = getThrowable(mock);
                //抛出业务类型的RpcException异常
                throw new RpcException(RpcException.BIZ_EXCEPTION, t);
            }
        //自定义Mock类，执行自定义逻辑
        } else { //impl mock
            try {
                //创建Invoker对象
                Invoker<T> invoker = getInvoker(mock);
                //执行Invoker对象的调用逻辑
                return invoker.invoke(invocation);
            } catch (Throwable t) {
                throw new RpcException("Failed to create mock implemention class " + mock, t);
            }
        }
    }

    private Throwable getThrowable(String throwstr) {
        // 从缓存中，获得 Throwable 对象
        Throwable throwable = (Throwable) throwables.get(throwstr);
        if (throwable != null) {
            return throwable;
        // 不存在，创建 Throwable 对象
        } else {
            Throwable t = null;
            try {
                // 获得异常类
                Class<?> bizException = ReflectUtils.forName(throwstr);
                // 获得构造方法
                Constructor<?> constructor;
                constructor = ReflectUtils.findConstructor(bizException, String.class);
                // 创建 Throwable 对象
                t = (Throwable) constructor.newInstance(new Object[]{" mocked exception for Service degradation. "});
                // 添加到缓存中
                if (throwables.size() < 1000) {
                    throwables.put(throwstr, t);
                }
            } catch (Exception e) {
                throw new RpcException("mock throw error :" + throwstr + " argument error.", e);
            }
            return t;
        }
    }

    @SuppressWarnings("unchecked")
    private Invoker<T> getInvoker(String mockService) {
        // 从缓存中，获得 Invoker 对象
        Invoker<T> invoker = (Invoker<T>) mocks.get(mockService);
        if (invoker != null) {
            return invoker;
        } else {
            // 不存在，创建 Invoker 对象
            // 1. 获得接口类
            Class<T> serviceType = (Class<T>) ReflectUtils.forName(url.getServiceInterface());
            // 2. 若为 `true` `default` ，修改修改为对应接口 + "Mock" 类。这种情况出现在原始 `mock = fail:true` 或 `mock = force:true` 等情况
            if (ConfigUtils.isDefault(mockService)) {
                mockService = serviceType.getName() + "Mock";
            }
            // 3. 获得 Mock 类
            Class<?> mockClass = ReflectUtils.forName(mockService);
            // 4. 校验 Mock 类，实现了接口类
            if (!serviceType.isAssignableFrom(mockClass)) {
                throw new IllegalArgumentException("The mock implemention class " + mockClass.getName() + " not implement interface " + serviceType.getName());
            }

            if (!serviceType.isAssignableFrom(mockClass)) {
                throw new IllegalArgumentException("The mock implemention class " + mockClass.getName() + " not implement interface " + serviceType.getName());
            }
            try {
                // 5. 创建 Mock 对象
                T mockObject = (T) mockClass.newInstance();
                // 6. 创建 Mock 对应，对应的 Invoker 对象
                invoker = proxyFactory.getInvoker(mockObject, (Class<T>) serviceType, url);
                // 7. 添加到缓存
                if (mocks.size() < 10000) {
                    mocks.put(mockService, invoker);
                }
                return invoker;
            } catch (InstantiationException e) {
                throw new IllegalStateException("No such empty constructor \"public " + mockClass.getSimpleName() + "()\" in mock implemention class " + mockClass.getName(), e);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    //mock=fail:throw
    //mock=fail:return
    //mock=xx.Service
    private String normallizeMock(String mock) {
        //若为空，直接返回
        if (mock == null || mock.trim().length() == 0) {
            return mock;
        //若结果为'true'、'default'、'fail'、'force'四种字符串，修改为对应接口+"Mock"类
        } else if (ConfigUtils.isDefault(mock) || "fail".equalsIgnoreCase(mock.trim()) || "force".equalsIgnoreCase(mock.trim())) {
            mock = url.getServiceInterface() + "Mock";
        }
        //若以'fail:'开头，去掉该开头
        if (mock.startsWith(Constants.FAIL_PREFIX)) {
            mock = mock.substring(Constants.FAIL_PREFIX.length()).trim();
        //若以'force:'开头，去掉该开头
        } else if (mock.startsWith(Constants.FORCE_PREFIX)) {
            mock = mock.substring(Constants.FORCE_PREFIX.length()).trim();
        }
        return mock;
    }

    public URL getUrl() {
        return this.url;
    }

    public boolean isAvailable() {
        return true;
    }

    public void destroy() {
        //do nothing
    }

    public Class<T> getInterface() {
        //FIXME
        return null;
    }
}