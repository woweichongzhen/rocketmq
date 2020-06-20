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
package org.apache.rocketmq.namesrv;

import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * namesrv入口
 */
public class NamesrvController {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private final NamesrvConfig namesrvConfig;

    private final NettyServerConfig nettyServerConfig;

    /**
     * 单线程定时任务，用于扫描未激活的broker
     */
    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
                    "NSScheduledThread"));

    /**
     * kv配置管理（json）
     */
    private final KVConfigManager kvConfigManager;

    /**
     * 路由信息管理
     */
    private final RouteInfoManager routeInfoManager;

    /**
     * 远端服务
     */
    private RemotingServer remotingServer;

    /**
     *
     */
    private BrokerHousekeepingService brokerHousekeepingService;

    /**
     * 远端线程池，处理请求
     */
    private ExecutorService remotingExecutor;

    /**
     * 配置对象
     */
    private Configuration configuration;

    /**
     * 文件监控服务
     */
    private FileWatchService fileWatchService;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        kvConfigManager = new KVConfigManager(this);
        routeInfoManager = new RouteInfoManager();
        brokerHousekeepingService = new BrokerHousekeepingService(this);
        configuration = new Configuration(
                log,
                this.namesrvConfig, this.nettyServerConfig
        );
        configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
    }

    /**
     * 初始化netty服务器，和其他管理对象，线程池等
     *
     * @return 初始化是否成功，true成功，false失败
     */
    public boolean initialize() {
        // 加载kv配置
        kvConfigManager.load();

        // 创建netty服务
        remotingServer = new NettyRemotingServer(nettyServerConfig, brokerHousekeepingService);

        // 远程通信线程池
        remotingExecutor = Executors.newFixedThreadPool(
                nettyServerConfig.getServerWorkerThreads(),
                new ThreadFactoryImpl("RemotingExecutorThread_"));

        // 注册默认的请求处理器
        this.registerProcessor();

        // 启动定时任务，扫描未激活的broker
        scheduledExecutorService.scheduleAtFixedRate(
                NamesrvController.this.routeInfoManager::scanNotActiveBroker,
                5,
                10,
                TimeUnit.SECONDS);

        // 启用定时任务，定时打印kv配置
        scheduledExecutorService.scheduleAtFixedRate(
                NamesrvController.this.kvConfigManager::printAllPeriodically,
                1,
                10,
                TimeUnit.MINUTES);

        // 启用TLS，使用加密模式
        if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
            // Register a listener to reload SslContext
            try {
                fileWatchService = new FileWatchService(
                        new String[]{
                                TlsSystemConfig.tlsServerCertPath,
                                TlsSystemConfig.tlsServerKeyPath,
                                TlsSystemConfig.tlsServerTrustCertPath
                        },
                        new FileWatchService.Listener() {
                            boolean certChanged, keyChanged = false;

                            @Override
                            public void onChanged(String path) {
                                if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                                    log.info("The trust certificate changed, reload the ssl context");
                                    reloadServerSslContext();
                                }
                                if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                                    certChanged = true;
                                }
                                if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                                    keyChanged = true;
                                }
                                if (certChanged && keyChanged) {
                                    log.info("The certificate and private key changed, reload the ssl context");
                                    certChanged = keyChanged = false;
                                    reloadServerSslContext();
                                }
                            }

                            private void reloadServerSslContext() {
                                ((NettyRemotingServer) remotingServer).loadSslContext();
                            }
                        });
            } catch (Exception e) {
                log.warn("FileWatchService created error, can't load the certificate dynamically");
            }
        }

        return true;
    }

    /**
     * 注册处理器
     */
    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {
            // 如果是集群测试，注册默认的集群测试请求处理器
            remotingServer.registerDefaultProcessor(
                    new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()),
                    remotingExecutor);
        } else {
            // 否则注册默认的请求处理器
            remotingServer.registerDefaultProcessor(
                    new DefaultRequestProcessor(this),
                    remotingExecutor);
        }
    }

    /**
     * 启动netty服务
     *
     * @throws Exception 启动异常
     */
    public void start() throws Exception {
        remotingServer.start();

        // 如果存在文件监控服务，启动
        if (fileWatchService != null) {
            fileWatchService.start();
        }
    }

    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingExecutor.shutdown();
        this.scheduledExecutorService.shutdown();

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
