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

package org.apache.rocketmq.logging;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 内部日志工厂
 */
public abstract class InternalLoggerFactory {

    /**
     * slf4j
     */
    public static final String LOGGER_SLF4J = "slf4j";

    /**
     * 内部
     */
    public static final String LOGGER_INNER = "inner";

    /**
     * 默认logger
     */
    public static final String DEFAULT_LOGGER = LOGGER_SLF4J;

    /**
     * logger类型
     */
    private static String loggerType = null;

    /**
     * logger工厂缓存
     * key：logger类型名称
     * value：logger工厂
     */
    private static ConcurrentHashMap<String, InternalLoggerFactory> loggerFactoryCache =
            new ConcurrentHashMap<String, InternalLoggerFactory>();

    public static InternalLogger getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    /**
     * 获取日志工厂
     *
     * @param name Logger名称
     * @return 内部Logger实例
     */
    public static InternalLogger getLogger(String name) {
        return getLoggerFactory().getLoggerInstance(name);
    }

    /**
     * 获取内部工厂实例
     *
     * @return 内部工厂实例
     */
    private static InternalLoggerFactory getLoggerFactory() {
        InternalLoggerFactory internalLoggerFactory = null;
        // 如果该类存在logger类型，从缓存中获取
        if (loggerType != null) {
            internalLoggerFactory = loggerFactoryCache.get(loggerType);
        }
        // 否则使用默认slf4j logger
        if (internalLoggerFactory == null) {
            internalLoggerFactory = loggerFactoryCache.get(DEFAULT_LOGGER);
        }
        // 否则使用 inner logger
        if (internalLoggerFactory == null) {
            internalLoggerFactory = loggerFactoryCache.get(LOGGER_INNER);
        }
        // 都找不到，抛出运行异常
        if (internalLoggerFactory == null) {
            throw new RuntimeException("[RocketMQ] Logger init failed, please check logger");
        }
        return internalLoggerFactory;
    }

    public static void setCurrentLoggerType(String type) {
        loggerType = type;
    }

    static {
        // 类初始化时，加载slf4j工厂和内部工厂
        try {
            new Slf4jLoggerFactory();
        } catch (Throwable e) {
            //ignore
        }
        try {
            new InnerLoggerFactory();
        } catch (Throwable e) {
            //ignore
        }
    }

    /**
     * 用于子类调用，注册日志工厂缓存
     */
    protected void doRegister() {
        // 获取logger类型
        String loggerType = getLoggerType();
        // 设置缓存
        if (loggerFactoryCache.get(loggerType) != null) {
            return;
        }
        loggerFactoryCache.put(loggerType, this);
    }

    protected abstract void shutdown();

    protected abstract InternalLogger getLoggerInstance(String name);

    /**
     * 获取Logger类型
     *
     * @return Logger类型
     */
    protected abstract String getLoggerType();
}
