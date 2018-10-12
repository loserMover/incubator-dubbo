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
package com.alibaba.dubbo.remoting.zookeeper.support;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.zookeeper.ChildListener;
import com.alibaba.dubbo.remoting.zookeeper.StateListener;
import com.alibaba.dubbo.remoting.zookeeper.ZookeeperClient;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

public abstract class AbstractZookeeperClient<TargetChildListener> implements ZookeeperClient {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractZookeeperClient.class);
    /**
     * @desc 注册中心URL
     */
    private final URL url;
    /**
     * @desc StateListener集合
     */
    private final Set<StateListener> stateListeners = new CopyOnWriteArraySet<StateListener>();
    /**
     * @desc childListener集合
     *
     * key1：节点路径
     * key2：ChildListener对象
     * value:监视器具体对象。不同Zookeeper客户端，实现会不同
     */
    private final ConcurrentMap<String, ConcurrentMap<ChildListener, TargetChildListener>> childListeners = new ConcurrentHashMap<String, ConcurrentMap<ChildListener, TargetChildListener>>();
    /**
     * @desc 是否关闭，默认关闭
     */
    private volatile boolean closed = false;

    public AbstractZookeeperClient(URL url) {
        this.url = url;
    }

    public URL getUrl() {
        return url;
    }

    public void create(String path, boolean ephemeral) {
        //循环创建父路径
        int i = path.lastIndexOf('/');
        if (i > 0) {
            String parentPath = path.substring(0, i);
            if (!checkExists(parentPath)) {//节点不存在
                create(parentPath, false);//创建节点
            }
        }
        //创建临时节点
        if (ephemeral) {
            createEphemeral(path);
        } else {//创建持久节点
            createPersistent(path);
        }
    }

    public void addStateListener(StateListener listener) {
        stateListeners.add(listener);
    }

    public void removeStateListener(StateListener listener) {
        stateListeners.remove(listener);
    }

    public Set<StateListener> getSessionListeners() {
        return stateListeners;
    }
    /**
     * @desc 添加ChildListener（添加节点监听器）
     * @param path 节点路径
     * @param listener 监听器
     * @return 子节点列表
     */
    public List<String> addChildListener(String path, final ChildListener listener) {
        //获得路径下的监听器数组
        ConcurrentMap<ChildListener, TargetChildListener> listeners = childListeners.get(path);
        if (listeners == null) {
            childListeners.putIfAbsent(path, new ConcurrentHashMap<ChildListener, TargetChildListener>());
            listeners = childListeners.get(path);
        }
        // 获得是否已经有该监听器
        TargetChildListener targetListener = listeners.get(listener);
        // 监听器不存在，进行创建
        if (targetListener == null) {
            listeners.putIfAbsent(listener, createTargetChildListener(path, listener));
            targetListener = listeners.get(listener);
        }
        // 向 Zookeeper ，真正发起订阅
        return addTargetChildListener(path, targetListener);
    }
    /**
     * @desc 移除ChildListener(移除节点监听器)
     * @param path 节点路径
     * @param listener 监听器
     */
    public void removeChildListener(String path, ChildListener listener) {
        ConcurrentMap<ChildListener, TargetChildListener> listeners = childListeners.get(path);
        if (listeners != null) {
            TargetChildListener targetListener = listeners.remove(listener);
            if (targetListener != null) {
                removeTargetChildListener(path, targetListener);
            }
        }
    }

    /**
     * @desc StateListener数组，回调
     * @param state 状态
     */
    protected void stateChanged(int state) {
        for (StateListener sessionListener : getSessionListeners()) {
            sessionListener.stateChanged(state);
        }
    }

    /**
     * @desc 关闭连接
     */
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            doClose();
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    /**
     * @desc 关闭 Zookeeper 连接
     */
    protected abstract void doClose();

    protected abstract void createPersistent(String path);

    protected abstract void createEphemeral(String path);

    protected abstract boolean checkExists(String path);

    /**
     * @desc 创建真正的 ChildListener 对象。因为，每个 Zookeeper 的库，实现不同
     * @param path 节点路径
     * @param listener 监听器
     * @return 真正监听对象
     */
    protected abstract TargetChildListener createTargetChildListener(String path, ChildListener listener);

    /**
     * @desc 向 Zookeeper ，真正发起订阅
     * @param path 节点路径
     * @param listener 监视器
     * @return 节点列表
     */
    protected abstract List<String> addTargetChildListener(String path, TargetChildListener listener);

    /**
     * @desc 向 Zookeeper ，真正发起取消订阅
     * @param path 节点路径
     * @param listener 监视器
     */
    protected abstract void removeTargetChildListener(String path, TargetChildListener listener);

}
