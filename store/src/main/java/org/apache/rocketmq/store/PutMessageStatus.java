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
package org.apache.rocketmq.store;

/**
 * 保存消息状态
 */
public enum PutMessageStatus {

    /**
     * 保存成功
     */
    PUT_OK,

    /**
     * flush磁盘超时
     */
    FLUSH_DISK_TIMEOUT,

    /**
     * slave flush 超时
     */
    FLUSH_SLAVE_TIMEOUT,

    /**
     * slave不可用
     */
    SLAVE_NOT_AVAILABLE,

    /**
     * 服务不可用
     */
    SERVICE_NOT_AVAILABLE,

    /**
     * 创建map文件失败
     */
    CREATE_MAPEDFILE_FAILED,

    /**
     * 消息异常
     */
    MESSAGE_ILLEGAL,

    /**
     * 属性大小超过
     */
    PROPERTIES_SIZE_EXCEEDED,

    /**
     * 操作系统 PageCahce 繁忙
     */
    OS_PAGECACHE_BUSY,

    /**
     * 未知异常
     */
    UNKNOWN_ERROR,
}
