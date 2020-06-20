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
package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.ExecutorService;

/**
 * 远程通信服务
 */
public interface RemotingServer extends RemotingService {

    /**
     * 注册处理器
     *
     * @param requestCode 请求码
     * @param processor   请求处理器
     * @param executor    处理请求线程池
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
                           final ExecutorService executor);

    /**
     * 注册默认的处理器
     *
     * @param processor 请求处理器
     * @param executor  处理请求线程池
     */
    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    int localListenPort();

    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    /**
     * 同步执行请求
     *
     * @param channel       通道
     * @param request       请求
     * @param timeoutMillis 超时时间
     * @return 返回
     * @throws InterruptedException         中断异常
     * @throws RemotingSendRequestException 发送请求异常
     * @throws RemotingTimeoutException     超时异常
     */
    RemotingCommand invokeSync(final Channel channel,
                               final RemotingCommand request,
                               final long timeoutMillis)
            throws
            InterruptedException,
            RemotingSendRequestException,
            RemotingTimeoutException;

    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
                     final InvokeCallback invokeCallback) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 单向发送请求
     *
     * @param channel       通道
     * @param request       请求
     * @param timeoutMillis 超时时间
     * @throws InterruptedException            中断异常
     * @throws RemotingTooMuchRequestException 远端收到太多请求异常（限流）
     * @throws RemotingTimeoutException        超时异常
     * @throws RemotingSendRequestException    发送请求异常
     */
    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
            RemotingSendRequestException;

}
