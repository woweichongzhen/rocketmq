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

package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 异步请求处理器
 */
public abstract class AsyncNettyRequestProcessor implements NettyRequestProcessor {

    /**
     * 异步处理请求
     *
     * @param ctx              通道上下文
     * @param request          请求
     * @param responseCallback 处理后的返回回调
     * @throws Exception 处理异常
     */
    public void asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request,
                                    RemotingResponseCallback responseCallback) throws Exception {
        // 先同步处理请求，然后触发回调方法
        RemotingCommand response = processRequest(ctx, request);
        responseCallback.callback(response);
    }
}
