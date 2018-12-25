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
package com.alibaba.dubbo.remoting.transport.netty;

import com.alibaba.dubbo.remoting.buffer.ChannelBuffer;
import com.alibaba.dubbo.remoting.buffer.ChannelBufferFactory;

import org.jboss.netty.buffer.ChannelBuffers;

import java.nio.ByteBuffer;

/**
 * Wrap netty dynamic channel buffer.
 */
public class NettyBackedChannelBufferFactory implements ChannelBufferFactory {

    private static final NettyBackedChannelBufferFactory INSTANCE = new NettyBackedChannelBufferFactory();

    public static ChannelBufferFactory getInstance() {
        return INSTANCE;
    }


    public ChannelBuffer getBuffer(int capacity) {
        return new NettyBackedChannelBuffer(ChannelBuffers.dynamicBuffer(capacity));//ChannelBuffers为'org.jboss.netty.buffer'包下
    }


    public ChannelBuffer getBuffer(byte[] array, int offset, int length) {
        //创建ChannelBuffer对象
        org.jboss.netty.buffer.ChannelBuffer buffer = ChannelBuffers.dynamicBuffer(length);
        //写入数据
        buffer.writeBytes(array, offset, length);
        //创建NettyBackedChannelBuffer对象
        return new NettyBackedChannelBuffer(buffer);
    }


    public ChannelBuffer getBuffer(ByteBuffer nioBuffer) {
        return new NettyBackedChannelBuffer(ChannelBuffers.wrappedBuffer(nioBuffer));//ChannelBuffers为'org.jboss.netty.buffer'包下
    }
}
