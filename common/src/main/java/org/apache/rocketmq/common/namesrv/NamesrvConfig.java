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

/**
 * $Id: NamesrvConfig.java 1839 2013-05-16 02:12:02Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.namesrv;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;

/**
 * namesrv配置
 */
public class NamesrvConfig {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    /**
     * 环境变量中主目录，优先从属性中获取，后从环境变量中获取
     */
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
            System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    /**
     * 存放kv配置json路径
     */
    private String kvConfigPath =
            System.getProperty("user.home") +
                    File.separator +
                    "namesrv" +
                    File.separator +
                    "kvConfig.json";

    /**
     * 自己的配置文件地址
     * 默认用户主目录/namesrv/namesrv.properties
     */
    private String configStorePath =
            System.getProperty("user.home") +
                    File.separator +
                    "namesrv" +
                    File.separator +
                    "namesrv.properties";

    /**
     * 生产环境名称，默认center
     */
    private String productEnvName = "center";

    /**
     * '
     * 是否为集群测试，默认false
     */
    private boolean clusterTest = false;

    /**
     * 是否支持有序消息，默认false
     */
    private boolean orderMessageEnable = false;

    public boolean isOrderMessageEnable() {
        return orderMessageEnable;
    }

    public void setOrderMessageEnable(boolean orderMessageEnable) {
        this.orderMessageEnable = orderMessageEnable;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public String getKvConfigPath() {
        return kvConfigPath;
    }

    public void setKvConfigPath(String kvConfigPath) {
        this.kvConfigPath = kvConfigPath;
    }

    public String getProductEnvName() {
        return productEnvName;
    }

    public void setProductEnvName(String productEnvName) {
        this.productEnvName = productEnvName;
    }

    public boolean isClusterTest() {
        return clusterTest;
    }

    public void setClusterTest(boolean clusterTest) {
        this.clusterTest = clusterTest;
    }

    public String getConfigStorePath() {
        return configStorePath;
    }

    public void setConfigStorePath(final String configStorePath) {
        this.configStorePath = configStorePath;
    }
}
