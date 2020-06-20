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

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * RPC回调钩子
 */
public interface RPCHook {

    /**
     * 请求之前的钩子
     *
     * @param remoteAddr 远端地址
     * @param request    远端请求
     */
    void doBeforeRequest(final String remoteAddr, final RemotingCommand request);

    /**
     * 返回之后的钩子
     *
     * @param remoteAddr 远端地址
     * @param request    远端请求
     * @param response   远端返回
     */
    void doAfterResponse(final String remoteAddr, final RemotingCommand request,
                         final RemotingCommand response);
}
