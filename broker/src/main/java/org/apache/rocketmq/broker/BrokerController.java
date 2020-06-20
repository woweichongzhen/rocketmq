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
package org.apache.rocketmq.broker;

import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.broker.client.*;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager;
import org.apache.rocketmq.broker.dledger.DLedgerRoleChangeHandler;
import org.apache.rocketmq.broker.filter.CommitLogDispatcherCalcBitMap;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filtersrv.FilterServerManager;
import org.apache.rocketmq.broker.latency.BrokerFastFailure;
import org.apache.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.broker.longpolling.NotifyMessageArrivingListener;
import org.apache.rocketmq.broker.longpolling.PullRequestHoldService;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.plugin.MessageStoreFactory;
import org.apache.rocketmq.broker.plugin.MessageStorePluginContext;
import org.apache.rocketmq.broker.processor.*;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.TransactionalMessageCheckService;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.broker.transaction.queue.DefaultTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageBridge;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl;
import org.apache.rocketmq.broker.util.ServiceProvider;
import org.apache.rocketmq.common.*;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.stats.MomentStatsItem;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.*;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.FileWatchService;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

/**
 * broker入口
 */
public class BrokerController {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final InternalLogger LOG_PROTECTION =
            InternalLoggerFactory.getLogger(LoggerName.PROTECTION_LOGGER_NAME);

    private static final InternalLogger LOG_WATER_MARK =
            InternalLoggerFactory.getLogger(LoggerName.WATER_MARK_LOGGER_NAME);

    /**
     * broker配置
     */
    private final BrokerConfig brokerConfig;

    /**
     * netty的broker server配置
     */
    private final NettyServerConfig nettyServerConfig;

    /**
     * netty的与 namesrv 连接的client配置
     */
    private final NettyClientConfig nettyClientConfig;

    /**
     * 消息存储配置
     */
    private final MessageStoreConfig messageStoreConfig;

    /**
     * 消费者offset管理
     */
    private final ConsumerOffsetManager consumerOffsetManager;

    /**
     * 消费者管理
     */
    private final ConsumerManager consumerManager;

    /**
     * 消费者过滤管理
     */
    private final ConsumerFilterManager consumerFilterManager;

    /**
     * 生产者管理
     */
    private final ProducerManager producerManager;
    private final ClientHousekeepingService clientHousekeepingService;

    /**
     * 推送消息处理器，处理Consumer过来拉取消息的请求
     */
    private final PullMessageProcessor pullMessageProcessor;

    /**
     * 拉取请求持有服务
     */
    private final PullRequestHoldService pullRequestHoldService;

    /**
     *
     */
    private final MessageArrivingListener messageArrivingListener;
    private final Broker2Client broker2Client;

    /**
     * 订阅的组管理
     */
    private final SubscriptionGroupManager subscriptionGroupManager;
    private final ConsumerIdsChangeListener consumerIdsChangeListener;
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    private final BrokerOuterAPI brokerOuterAPI;

    /**
     * 内部定时任务线程池
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("BrokerControllerScheduledThread"));

    /**
     * 主从同步
     */
    private final SlaveSynchronize slaveSynchronize;

    /**
     * 发送消息线程池阻塞队列
     */
    private final BlockingQueue<Runnable> sendThreadPoolQueue;

    /**
     * 处理拉取消息线程池阻塞队列
     */
    private final BlockingQueue<Runnable> pullThreadPoolQueue;

    /**
     * 回复消息线程池阻塞队列
     */
    private final BlockingQueue<Runnable> replyThreadPoolQueue;

    /**
     * 查询消息线程池阻塞队列
     */
    private final BlockingQueue<Runnable> queryThreadPoolQueue;

    /**
     * 客户端管理线程池阻塞队列
     */
    private final BlockingQueue<Runnable> clientManagerThreadPoolQueue;

    /**
     * 心跳线程池阻塞队列
     */
    private final BlockingQueue<Runnable> heartbeatThreadPoolQueue;

    /**
     * 消费者管理线程池阻塞队列
     */
    private final BlockingQueue<Runnable> consumerManagerThreadPoolQueue;

    /**
     * 最终事务线程池阻塞队列
     */
    private final BlockingQueue<Runnable> endTransactionThreadPoolQueue;
    private final FilterServerManager filterServerManager;

    /**
     * broker状态管理，metric 组件
     */
    private final BrokerStatsManager brokerStatsManager;
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();
    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();

    /**
     * 消息存储组件
     */
    private MessageStore messageStore;
    private RemotingServer remotingServer;
    private RemotingServer fastRemotingServer;

    /**
     * topic配置管理
     */
    private TopicConfigManager topicConfigManager;
    private ExecutorService sendMessageExecutor;
    private ExecutorService pullMessageExecutor;
    private ExecutorService replyMessageExecutor;
    private ExecutorService queryMessageExecutor;
    private ExecutorService adminBrokerExecutor;
    private ExecutorService clientManageExecutor;
    private ExecutorService heartbeatExecutor;
    private ExecutorService consumerManageExecutor;
    private ExecutorService endTransactionExecutor;

    /**
     * 是否周期更新master高可用服务地址
     */
    private boolean updateMasterHAServerAddrPeriodically = false;

    /**
     * broker统计组件
     */
    private BrokerStats brokerStats;

    /**
     * 存储host
     */
    private InetSocketAddress storeHost;

    /**
     * broker快速失败
     */
    private BrokerFastFailure brokerFastFailure;

    /**
     * 配置存储
     */
    private Configuration configuration;
    private FileWatchService fileWatchService;
    private TransactionalMessageCheckService transactionalMessageCheckService;
    private TransactionalMessageService transactionalMessageService;
    private AbstractTransactionalMessageCheckListener transactionalMessageCheckListener;
    private Future<?> slaveSyncFuture;
    private Map<Class, AccessValidator> accessValidatorMap = new HashMap<Class, AccessValidator>();

    public BrokerController(
            final BrokerConfig brokerConfig,
            final NettyServerConfig nettyServerConfig,
            final NettyClientConfig nettyClientConfig,
            final MessageStoreConfig messageStoreConfig) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.messageStoreConfig = messageStoreConfig;
        consumerOffsetManager = new ConsumerOffsetManager(this);
        topicConfigManager = new TopicConfigManager(this);
        pullMessageProcessor = new PullMessageProcessor(this);
        pullRequestHoldService = new PullRequestHoldService(this);
        messageArrivingListener = new NotifyMessageArrivingListener(pullRequestHoldService);
        consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        consumerManager = new ConsumerManager(consumerIdsChangeListener);
        consumerFilterManager = new ConsumerFilterManager(this);
        producerManager = new ProducerManager();
        clientHousekeepingService = new ClientHousekeepingService(this);
        broker2Client = new Broker2Client(this);
        subscriptionGroupManager = new SubscriptionGroupManager(this);
        brokerOuterAPI = new BrokerOuterAPI(this.nettyClientConfig);
        filterServerManager = new FilterServerManager(this);

        slaveSynchronize = new SlaveSynchronize(this);

        sendThreadPoolQueue =
                new LinkedBlockingQueue<>(this.brokerConfig.getSendThreadPoolQueueCapacity());
        pullThreadPoolQueue =
                new LinkedBlockingQueue<>(this.brokerConfig.getPullThreadPoolQueueCapacity());
        replyThreadPoolQueue =
                new LinkedBlockingQueue<>(this.brokerConfig.getReplyThreadPoolQueueCapacity());
        queryThreadPoolQueue =
                new LinkedBlockingQueue<>(this.brokerConfig.getQueryThreadPoolQueueCapacity());
        clientManagerThreadPoolQueue =
                new LinkedBlockingQueue<>(this.brokerConfig.getClientManagerThreadPoolQueueCapacity());
        consumerManagerThreadPoolQueue =
                new LinkedBlockingQueue<>(this.brokerConfig.getConsumerManagerThreadPoolQueueCapacity());
        heartbeatThreadPoolQueue =
                new LinkedBlockingQueue<>(this.brokerConfig.getHeartbeatThreadPoolQueueCapacity());
        endTransactionThreadPoolQueue =
                new LinkedBlockingQueue<>(this.brokerConfig.getEndTransactionPoolQueueCapacity());

        brokerStatsManager = new BrokerStatsManager(this.brokerConfig.getBrokerClusterName());

        // 获取broker的IP1配置和netty监听端口，设置到存储host中
        setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(),
                this.getNettyServerConfig().getListenPort()));

        brokerFastFailure = new BrokerFastFailure(this);
        configuration = new Configuration(
                log,
                BrokerPathConfigHelper.getBrokerConfigPath(),
                this.brokerConfig, this.nettyServerConfig, this.nettyClientConfig, this.messageStoreConfig
        );
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public BlockingQueue<Runnable> getPullThreadPoolQueue() {
        return pullThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getQueryThreadPoolQueue() {
        return queryThreadPoolQueue;
    }

    /**
     * 初始化broker入口
     *
     * @return true初始化成功
     * @throws CloneNotSupportedException 不支持克隆异常
     */
    public boolean initialize() throws CloneNotSupportedException {
        // 加载磁盘数据到文件，topic配置，消费者offset，订阅的生产者消费者组，消费者过滤
        boolean result = topicConfigManager.load();
        result = result && consumerOffsetManager.load();
        result = result && subscriptionGroupManager.load();
        result = result && consumerFilterManager.load();

        if (result) {
            // 前面都加载成功了，创建默认的消息存储组件
            try {
                messageStore = new DefaultMessageStore(
                        messageStoreConfig,
                        brokerStatsManager,
                        messageArrivingListener,
                        brokerConfig);

                // 如果启用了 DLeger 管理 CommitLog ，则创建角色改变处理器
                if (messageStoreConfig.isEnableDLegerCommitLog()) {
                    DLedgerRoleChangeHandler roleChangeHandler = new DLedgerRoleChangeHandler(
                            this,
                            (DefaultMessageStore) messageStore);
                    ((DLedgerCommitLog) ((DefaultMessageStore) messageStore).getCommitLog())
                            .getdLedgerServer()
                            .getdLedgerLeaderElector()
                            .addRoleChangeHandler(roleChangeHandler);
                }

                // 创建broker统计组件
                brokerStats = new BrokerStats((DefaultMessageStore) messageStore);

                // 创建消息存储插件上下文，对消息存储组件做一些配置
                MessageStorePluginContext context = new MessageStorePluginContext(
                        messageStoreConfig,
                        brokerStatsManager,
                        messageArrivingListener,
                        brokerConfig);
                messageStore = MessageStoreFactory.build(context, messageStore);
                messageStore.getDispatcherList()
                        .addFirst(new CommitLogDispatcherCalcBitMap(brokerConfig, consumerFilterManager));
            } catch (IOException e) {
                result = false;
                log.error("Failed to initialize", e);
            }
        }

        // 如果前面都处理成功了，加载消息存储
        result = result && messageStore.load();

        if (result) {
            // 前面都处理成功了，开始初始化Broker的Server
            remotingServer = new NettyRemotingServer(nettyServerConfig, clientHousekeepingService);

            // 再初始化一个fast远端服务，端口为broker端口减2
            NettyServerConfig fastConfig = (NettyServerConfig) nettyServerConfig.clone();
            fastConfig.setListenPort(nettyServerConfig.getListenPort() - 2);
            fastRemotingServer = new NettyRemotingServer(fastConfig, clientHousekeepingService);

            // 初始化消息发送，拉取消息（接收客户端拉取消息任务），回复消息，查询消息
            // 管理broker，客户端管理，心跳，终止任务，消费者管理线程池
            sendMessageExecutor = new BrokerFixedThreadPoolExecutor(
                    brokerConfig.getSendMessageThreadPoolNums(),
                    brokerConfig.getSendMessageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    sendThreadPoolQueue,
                    new ThreadFactoryImpl("SendMessageThread_"));

            pullMessageExecutor = new BrokerFixedThreadPoolExecutor(
                    brokerConfig.getPullMessageThreadPoolNums(),
                    brokerConfig.getPullMessageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    pullThreadPoolQueue,
                    new ThreadFactoryImpl("PullMessageThread_"));

            replyMessageExecutor = new BrokerFixedThreadPoolExecutor(
                    brokerConfig.getProcessReplyMessageThreadPoolNums(),
                    brokerConfig.getProcessReplyMessageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    replyThreadPoolQueue,
                    new ThreadFactoryImpl("ProcessReplyMessageThread_"));

            queryMessageExecutor = new BrokerFixedThreadPoolExecutor(
                    brokerConfig.getQueryMessageThreadPoolNums(),
                    brokerConfig.getQueryMessageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    queryThreadPoolQueue,
                    new ThreadFactoryImpl("QueryMessageThread_"));

            adminBrokerExecutor =
                    Executors.newFixedThreadPool(brokerConfig.getAdminBrokerThreadPoolNums(),
                            new ThreadFactoryImpl(
                                    "AdminBrokerThread_"));

            clientManageExecutor = new ThreadPoolExecutor(
                    brokerConfig.getClientManageThreadPoolNums(),
                    brokerConfig.getClientManageThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    clientManagerThreadPoolQueue,
                    new ThreadFactoryImpl("ClientManageThread_"));

            heartbeatExecutor = new BrokerFixedThreadPoolExecutor(
                    brokerConfig.getHeartbeatThreadPoolNums(),
                    brokerConfig.getHeartbeatThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    heartbeatThreadPoolQueue,
                    new ThreadFactoryImpl("HeartbeatThread_", true));

            endTransactionExecutor = new BrokerFixedThreadPoolExecutor(
                    brokerConfig.getEndTransactionThreadPoolNums(),
                    brokerConfig.getEndTransactionThreadPoolNums(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    endTransactionThreadPoolQueue,
                    new ThreadFactoryImpl("EndTransactionThread_"));

            consumerManageExecutor = Executors.newFixedThreadPool(
                    brokerConfig.getConsumerManageThreadPoolNums(),
                    new ThreadFactoryImpl("ConsumerManageThread_"));

            // 注册处理器
            this.registerProcessor();

            // 比较和下一天早上的时间差
            final long initialDelay = UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis();
            // 24小时
            final long period = 1000 * 60 * 60 * 24;
            // 执行24小时的统计定时任务
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    BrokerController.this.getBrokerStats().record();
                } catch (Throwable e) {
                    log.error("schedule record error.", e);
                }
            }, initialDelay, period, TimeUnit.MILLISECONDS);

            // 执行消费者offset持久化定时任务
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    BrokerController.this.consumerOffsetManager.persist();
                } catch (Throwable e) {
                    log.error("schedule persist consumerOffset error.", e);
                }
            }, 1000 * 10, brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

            // 执行消费者过滤持久化定时任务，10S执行一次
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    BrokerController.this.consumerFilterManager.persist();
                } catch (Throwable e) {
                    log.error("schedule persist consumer filter error.", e);
                }
            }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);

            // 执行broker保护定时任务，3分钟一次
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    BrokerController.this.protectBroker();
                } catch (Throwable e) {
                    log.error("protectBroker error.", e);
                }
            }, 3, 3, TimeUnit.MINUTES);

            // 打印水位定时任务，1分钟一次
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    BrokerController.this.printWaterMark();
                } catch (Throwable e) {
                    log.error("printWaterMark error.", e);
                }
            }, 10, 1, TimeUnit.SECONDS);

            // 落后的CommitLog分发任务，1分钟一次
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    log.info("dispatch behind commit log {} bytes",
                            BrokerController.this.getMessageStore().dispatchBehindBytes());
                } catch (Throwable e) {
                    log.error("schedule dispatchBehindBytes error.", e);
                }
            }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);

            // 如果配置了namesrv，更新namesrv地址列表，同时发送请求给namesrv
            if (brokerConfig.getNamesrvAddr() != null) {
                brokerOuterAPI.updateNameServerAddressList(brokerConfig.getNamesrvAddr());
                log.info("Set user specified name server address: {}", brokerConfig.getNamesrvAddr());
            } else if (brokerConfig.isFetchNamesrvAddrByAddressServer()) {
                // 如果启用了强制拉取地址任务，每2分钟强制拉取一次
                scheduledExecutorService.scheduleAtFixedRate(() -> {
                    try {
                        BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                    } catch (Throwable e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
            }

            if (!messageStoreConfig.isEnableDLegerCommitLog()) {
                // 如果没有启用 DLeger 管理 CommitLog
                if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                    // 如果为 SLAVE 节点，则获取HA master地址，并且长度要大于6，随后更新HA master地址
                    if (messageStoreConfig.getHaMasterAddress() != null && messageStoreConfig.getHaMasterAddress().length() >= 6) {
                        messageStore.updateHaMasterAddress(messageStoreConfig.getHaMasterAddress());
                        updateMasterHAServerAddrPeriodically = false;
                    } else {
                        updateMasterHAServerAddrPeriodically = true;
                    }
                } else {
                    // 如果为 MASTER 角色，每分钟定时任务，打印Master和Slave的差异
                    scheduledExecutorService.scheduleAtFixedRate(() -> {
                        try {
                            BrokerController.this.printMasterAndSlaveDiff();
                        } catch (Throwable e) {
                            log.error("schedule printMasterAndSlaveDiff error.", e);
                        }
                    }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
                }
            }

            // 启用TLS加密传输模式
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
                                    ((NettyRemotingServer) fastRemotingServer).loadSslContext();
                                }
                            });
                } catch (Exception e) {
                    log.warn("FileWatchService created error, can't load the certificate dynamically");
                }
            }

            // 初始化事务
            this.initialTransaction();

            // 初始化ACL
            this.initialAcl();

            // 初始化RPC回调钩子
            this.initialRpcHooks();
        }
        return result;
    }

    /**
     * 初始化事务
     */
    private void initialTransaction() {
        transactionalMessageService = ServiceProvider.loadClass(ServiceProvider.TRANSACTION_SERVICE_ID,
                TransactionalMessageService.class);
        if (null == transactionalMessageService) {
            transactionalMessageService =
                    new TransactionalMessageServiceImpl(new TransactionalMessageBridge(this, getMessageStore()));
            log.warn("Load default transaction message hook service: {}",
                    TransactionalMessageServiceImpl.class.getSimpleName());
        }
        transactionalMessageCheckListener = ServiceProvider.loadClass(ServiceProvider.TRANSACTION_LISTENER_ID,
                AbstractTransactionalMessageCheckListener.class);
        if (null == transactionalMessageCheckListener) {
            transactionalMessageCheckListener = new DefaultTransactionalMessageCheckListener();
            log.warn("Load default discard message hook service: {}",
                    DefaultTransactionalMessageCheckListener.class.getSimpleName());
        }
        transactionalMessageCheckListener.setBrokerController(this);
        transactionalMessageCheckService = new TransactionalMessageCheckService(this);
    }

    /**
     * 初始化ACL
     */
    private void initialAcl() {
        if (!brokerConfig.isAclEnable()) {
            log.info("The broker dose not enable acl");
            return;
        }

        List<AccessValidator> accessValidators = ServiceProvider.load(ServiceProvider.ACL_VALIDATOR_ID,
                AccessValidator.class);
        if (accessValidators == null || accessValidators.isEmpty()) {
            log.info("The broker dose not load the AccessValidator");
            return;
        }

        for (AccessValidator accessValidator : accessValidators) {
            final AccessValidator validator = accessValidator;
            accessValidatorMap.put(validator.getClass(), validator);
            registerServerRPCHook(new RPCHook() {

                @Override
                public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
                    //Do not catch the exception
                    validator.validate(validator.parse(request, remoteAddr));
                }

                @Override
                public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
                }
            });
        }
    }

    /**
     * 初始化RPC回调钩子
     */
    private void initialRpcHooks() {
        List<RPCHook> rpcHooks = ServiceProvider.load(ServiceProvider.RPC_HOOK_ID, RPCHook.class);
        if (rpcHooks == null || rpcHooks.isEmpty()) {
            return;
        }
        for (RPCHook rpcHook : rpcHooks) {
            registerServerRPCHook(rpcHook);
        }
    }

    /**
     * 注册处理器
     */
    public void registerProcessor() {
        // 注册发送消息处理器，以及发送的回调钩子，消费者的回调钩子
        SendMessageProcessor sendProcessor = new SendMessageProcessor(this);
        sendProcessor.registerSendMessageHook(sendMessageHookList);
        sendProcessor.registerConsumeMessageHook(consumeMessageHookList);

        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendProcessor, sendMessageExecutor);
        remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, sendMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, sendMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, sendMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendProcessor, sendMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, sendMessageExecutor);

        // 注册拉取消息处理器，以及拉取消息的回调钩子
        remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, pullMessageProcessor, pullMessageExecutor);
        pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        // 注册回复消息处理器，以及回调钩子
        ReplyMessageProcessor replyMessageProcessor = new ReplyMessageProcessor(this);
        replyMessageProcessor.registerSendMessageHook(sendMessageHookList);

        remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
        remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor,
                replyMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor,
                replyMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor,
                replyMessageExecutor);

        // 注册查询处理器
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, queryMessageExecutor);
        remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, queryMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, queryMessageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, queryMessageExecutor);

        // 注册客户端管理处理器
        ClientManageProcessor clientProcessor = new ClientManageProcessor(this);
        remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, heartbeatExecutor);
        remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, clientManageExecutor);
        remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientProcessor, clientManageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, heartbeatExecutor);
        fastRemotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, clientManageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientProcessor, clientManageExecutor);

        // 注册消费者管理处理器
        ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(this);
        remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor,
                consumerManageExecutor);
        remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor,
                consumerManageExecutor);
        remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor,
                consumerManageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor,
                consumerManageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor,
                consumerManageExecutor);
        fastRemotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor,
                consumerManageExecutor);

        // 注册终止事务处理器
        remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this),
                endTransactionExecutor);
        fastRemotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this),
                endTransactionExecutor);

        // 注册管理broker的默认处理器
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        remotingServer.registerDefaultProcessor(adminProcessor, adminBrokerExecutor);
        fastRemotingServer.registerDefaultProcessor(adminProcessor, adminBrokerExecutor);
    }

    public BrokerStats getBrokerStats() {
        return brokerStats;
    }

    public void setBrokerStats(BrokerStats brokerStats) {
        this.brokerStats = brokerStats;
    }

    public void protectBroker() {
        if (brokerConfig.isDisableConsumeIfConsumerReadSlowly()) {
            final Iterator<Map.Entry<String, MomentStatsItem>> it =
                    brokerStatsManager.getMomentStatsItemSetFallSize().getStatsItemTable().entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<String, MomentStatsItem> next = it.next();
                final long fallBehindBytes = next.getValue().getValue().get();
                if (fallBehindBytes > brokerConfig.getConsumerFallbehindThreshold()) {
                    final String[] split = next.getValue().getStatsKey().split("@");
                    final String group = split[2];
                    LOG_PROTECTION.info("[PROTECT_BROKER] the consumer[{}] consume slowly, {} bytes, disable it",
                            group, fallBehindBytes);
                    subscriptionGroupManager.disableConsume(group);
                }
            }
        }
    }

    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable peek = q.peek();
        if (peek != null) {
            RequestTask rt = BrokerFastFailure.castRunnable(peek);
            slowTimeMills = rt == null ? 0 : messageStore.now() - rt.getCreateTimestamp();
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    public long headSlowTimeMills4SendThreadPoolQueue() {
        return headSlowTimeMills(sendThreadPoolQueue);
    }

    public long headSlowTimeMills4PullThreadPoolQueue() {
        return headSlowTimeMills(pullThreadPoolQueue);
    }

    public long headSlowTimeMills4QueryThreadPoolQueue() {
        return headSlowTimeMills(queryThreadPoolQueue);
    }

    public long headSlowTimeMills4EndTransactionThreadPoolQueue() {
        return headSlowTimeMills(endTransactionThreadPoolQueue);
    }

    public void printWaterMark() {
        LOG_WATER_MARK.info("[WATERMARK] Send Queue Size: {} SlowTimeMills: {}", sendThreadPoolQueue.size(),
                headSlowTimeMills4SendThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Pull Queue Size: {} SlowTimeMills: {}", pullThreadPoolQueue.size(),
                headSlowTimeMills4PullThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Query Queue Size: {} SlowTimeMills: {}", queryThreadPoolQueue.size(),
                headSlowTimeMills4QueryThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Transaction Queue Size: {} SlowTimeMills: {}",
                endTransactionThreadPoolQueue.size(), headSlowTimeMills4EndTransactionThreadPoolQueue());
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    private void printMasterAndSlaveDiff() {
        long diff = messageStore.slaveFallBehindMuch();

        // XXX: warn and notify me
        log.info("Slave fall behind master: {} bytes", diff);
    }

    public Broker2Client getBroker2Client() {
        return broker2Client;
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public ConsumerFilterManager getConsumerFilterManager() {
        return consumerFilterManager;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public void setFastRemotingServer(RemotingServer fastRemotingServer) {
        this.fastRemotingServer = fastRemotingServer;
    }

    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }

    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public void shutdown() {
        if (brokerStatsManager != null) {
            brokerStatsManager.shutdown();
        }

        if (clientHousekeepingService != null) {
            clientHousekeepingService.shutdown();
        }

        if (pullRequestHoldService != null) {
            pullRequestHoldService.shutdown();
        }

        if (remotingServer != null) {
            remotingServer.shutdown();
        }

        if (fastRemotingServer != null) {
            fastRemotingServer.shutdown();
        }

        if (fileWatchService != null) {
            fileWatchService.shutdown();
        }

        if (messageStore != null) {
            messageStore.shutdown();
        }

        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }

        unregisterBrokerAll();

        if (sendMessageExecutor != null) {
            sendMessageExecutor.shutdown();
        }

        if (pullMessageExecutor != null) {
            pullMessageExecutor.shutdown();
        }

        if (replyMessageExecutor != null) {
            replyMessageExecutor.shutdown();
        }

        if (adminBrokerExecutor != null) {
            adminBrokerExecutor.shutdown();
        }

        if (brokerOuterAPI != null) {
            brokerOuterAPI.shutdown();
        }

        consumerOffsetManager.persist();

        if (filterServerManager != null) {
            filterServerManager.shutdown();
        }

        if (brokerFastFailure != null) {
            brokerFastFailure.shutdown();
        }

        if (consumerFilterManager != null) {
            consumerFilterManager.persist();
        }

        if (clientManageExecutor != null) {
            clientManageExecutor.shutdown();
        }

        if (queryMessageExecutor != null) {
            queryMessageExecutor.shutdown();
        }

        if (consumerManageExecutor != null) {
            consumerManageExecutor.shutdown();
        }

        if (fileWatchService != null) {
            fileWatchService.shutdown();
        }
        if (transactionalMessageCheckService != null) {
            transactionalMessageCheckService.shutdown(false);
        }

        if (endTransactionExecutor != null) {
            endTransactionExecutor.shutdown();
        }
    }

    private void unregisterBrokerAll() {
        brokerOuterAPI.unregisterBrokerAll(
                brokerConfig.getBrokerClusterName(),
                getBrokerAddr(),
                brokerConfig.getBrokerName(),
                brokerConfig.getBrokerId());
    }

    public String getBrokerAddr() {
        return brokerConfig.getBrokerIP1() + ":" + nettyServerConfig.getListenPort();
    }

    /**
     * 启动入口
     *
     * @throws Exception 启动异常
     */
    public void start() throws Exception {
        // 启动消息存储组件
        if (messageStore != null) {
            messageStore.start();
        }

        // 启动broker远端服务组件
        if (remotingServer != null) {
            remotingServer.start();
        }
        if (fastRemotingServer != null) {
            fastRemotingServer.start();
        }

        // 文件监控组件启动
        if (fileWatchService != null) {
            fileWatchService.start();
        }

        // broker 向外请求的API组件启动
        if (brokerOuterAPI != null) {
            brokerOuterAPI.start();
        }

        // 拉取请求持有服务启动
        if (pullRequestHoldService != null) {
            pullRequestHoldService.start();
        }

        if (clientHousekeepingService != null) {
            clientHousekeepingService.start();
        }

        if (filterServerManager != null) {
            filterServerManager.start();
        }

        // 如果未开启 DLeger 管理 CommitLog ，启动本身的角色，处理主从同步，注册 broker 到 Namesrv
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.startProcessorByHa(messageStoreConfig.getBrokerRole());
            this.handleSlaveSynchronize(messageStoreConfig.getBrokerRole());
            this.registerBrokerAll(true, false, true);
        }

        // 启动定时任务，注册broker到所有的Namesrv，注册频率，10-60S之间
        scheduledExecutorService.scheduleAtFixedRate(() -> {
                    try {
                        BrokerController.this.registerBrokerAll(
                                true,
                                false,
                                brokerConfig.isForceRegister());
                    } catch (Throwable e) {
                        log.error("registerBrokerAll Exception", e);
                    }
                },
                1000 * 10,
                Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)),
                TimeUnit.MILLISECONDS);

        // broker状态和快速失败启动
        if (brokerStatsManager != null) {
            brokerStatsManager.start();
        }
        if (brokerFastFailure != null) {
            brokerFastFailure.start();
        }
    }

    public synchronized void registerIncrementBrokerData(TopicConfig topicConfig, DataVersion dataVersion) {
        TopicConfig registerTopicConfig = topicConfig;
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            registerTopicConfig =
                    new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(),
                            topicConfig.getWriteQueueNums(),
                            brokerConfig.getBrokerPermission());
        }

        ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
        topicConfigTable.put(topicConfig.getTopicName(), registerTopicConfig);
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setDataVersion(dataVersion);
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

        doRegisterBrokerAll(true, false, topicConfigSerializeWrapper);
    }

    /**
     * 注册broker到Namesrv中
     *
     * @param checkOrderConfig 是否检测顺序配置
     * @param oneway           是否单向
     * @param forceRegister    是否强制注册
     */
    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway, boolean forceRegister) {
        // topic配置序列化包装
        TopicConfigSerializeWrapper topicConfigWrapper =
                this.getTopicConfigManager().buildTopicConfigSerializeWrapper();

        // topic配置，先判断broker的读写权限，然后设置配置表到配置包装类中
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                TopicConfig tmp = new TopicConfig(
                        topicConfig.getTopicName(),
                        topicConfig.getReadQueueNums(),
                        topicConfig.getWriteQueueNums(),
                        brokerConfig.getBrokerPermission());
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        // 如果强制注册，或者需要注册
        if (forceRegister || needRegister(brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                brokerConfig.getBrokerName(),
                brokerConfig.getBrokerId(),
                brokerConfig.getRegisterBrokerTimeoutMills())) {
            // 注册broker到所有Namesrv中
            this.doRegisterBrokerAll(checkOrderConfig, oneway, topicConfigWrapper);
        }
    }

    /**
     * 注册broker到所有Namesrv中
     *
     * @param checkOrderConfig   是否检查顺序配置
     * @param oneway             是否单向
     * @param topicConfigWrapper topic配置包装
     */
    private void doRegisterBrokerAll(boolean checkOrderConfig,
                                     boolean oneway,
                                     TopicConfigSerializeWrapper topicConfigWrapper) {
        // 注册到所有Namesrv上
        List<RegisterBrokerResult> registerBrokerResultList = brokerOuterAPI.registerBrokerAll(
                brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                brokerConfig.getBrokerName(),
                brokerConfig.getBrokerId(),
                this.getHAServerAddr(),
                topicConfigWrapper,
                filterServerManager.buildNewFilterServerList(),
                oneway,
                brokerConfig.getRegisterBrokerTimeoutMills(),
                brokerConfig.isCompressedRegister());

        // 如果注册列表大于0，只判断第一个就可以了
        if (registerBrokerResultList.size() > 0) {
            RegisterBrokerResult registerBrokerResult = registerBrokerResultList.get(0);
            if (registerBrokerResult != null) {
                // 如果需要周期更新masterHA服务地址，并且ha服务地址不为空，则为消息存储组件设置HA master地址
                if (updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
                    messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
                }

                // 设置SLAVE同步服务的master地址
                slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());

                // 如果检测配置顺序，更新topic顺序配置
                if (checkOrderConfig) {
                    this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
                }
            }
        }
    }

    /**
     * 是否需要注册
     *
     * @param clusterName  集群名称
     * @param brokerAddr   broker地址
     * @param brokerName   broker名称
     * @param brokerId     brokerId
     * @param timeoutMills 超时时间
     * @return true需要注册
     */
    private boolean needRegister(final String clusterName,
                                 final String brokerAddr,
                                 final String brokerName,
                                 final long brokerId,
                                 final int timeoutMills) {
        // 获取topic配置序列化包装
        TopicConfigSerializeWrapper topicConfigWrapper =
                this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
        // 请求判断是否需要注册
        List<Boolean> changeList = brokerOuterAPI.needRegister(clusterName, brokerAddr, brokerName, brokerId,
                topicConfigWrapper, timeoutMills);
        // 遍历返回，只要有一个为true，就需要注册
        boolean needRegister = false;
        for (Boolean changed : changeList) {
            if (changed) {
                needRegister = true;
                break;
            }
        }
        return needRegister;
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public String getHAServerAddr() {
        return brokerConfig.getBrokerIP2() + ":" + messageStoreConfig.getHaListenPort();
    }

    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }

    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }

    public ExecutorService getPullMessageExecutor() {
        return pullMessageExecutor;
    }

    public void setPullMessageExecutor(ExecutorService pullMessageExecutor) {
        this.pullMessageExecutor = pullMessageExecutor;
    }

    public BlockingQueue<Runnable> getSendThreadPoolQueue() {
        return sendThreadPoolQueue;
    }

    public FilterServerManager getFilterServerManager() {
        return filterServerManager;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public List<SendMessageHook> getSendMessageHookList() {
        return sendMessageHookList;
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        sendMessageHookList.add(hook);
        log.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    public List<ConsumeMessageHook> getConsumeMessageHookList() {
        return consumeMessageHookList;
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        consumeMessageHookList.add(hook);
        log.info("register ConsumeMessageHook Hook, {}", hook.hookName());
    }

    public void registerServerRPCHook(RPCHook rpcHook) {
        getRemotingServer().registerRPCHook(rpcHook);
        fastRemotingServer.registerRPCHook(rpcHook);
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }

    public InetSocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public BlockingQueue<Runnable> getHeartbeatThreadPoolQueue() {
        return heartbeatThreadPoolQueue;
    }

    public TransactionalMessageCheckService getTransactionalMessageCheckService() {
        return transactionalMessageCheckService;
    }

    public void setTransactionalMessageCheckService(
            TransactionalMessageCheckService transactionalMessageCheckService) {
        this.transactionalMessageCheckService = transactionalMessageCheckService;
    }

    public TransactionalMessageService getTransactionalMessageService() {
        return transactionalMessageService;
    }

    public void setTransactionalMessageService(TransactionalMessageService transactionalMessageService) {
        this.transactionalMessageService = transactionalMessageService;
    }

    public AbstractTransactionalMessageCheckListener getTransactionalMessageCheckListener() {
        return transactionalMessageCheckListener;
    }

    public void setTransactionalMessageCheckListener(
            AbstractTransactionalMessageCheckListener transactionalMessageCheckListener) {
        this.transactionalMessageCheckListener = transactionalMessageCheckListener;
    }


    public BlockingQueue<Runnable> getEndTransactionThreadPoolQueue() {
        return endTransactionThreadPoolQueue;

    }

    public Map<Class, AccessValidator> getAccessValidatorMap() {
        return accessValidatorMap;
    }

    private void handleSlaveSynchronize(BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            if (null != slaveSyncFuture) {
                slaveSyncFuture.cancel(false);
            }
            slaveSynchronize.setMasterAddr(null);
            slaveSyncFuture = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.slaveSynchronize.syncAll();
                    } catch (Throwable e) {
                        log.error("ScheduledTask SlaveSynchronize syncAll error.", e);
                    }
                }
            }, 1000 * 3, 1000 * 10, TimeUnit.MILLISECONDS);
        } else {
            //handle the slave synchronise
            if (null != slaveSyncFuture) {
                slaveSyncFuture.cancel(false);
            }
            slaveSynchronize.setMasterAddr(null);
        }
    }

    public void changeToSlave(int brokerId) {
        log.info("Begin to change to slave brokerName={} brokerId={}", brokerConfig.getBrokerName(), brokerId);

        //change the role
        brokerConfig.setBrokerId(brokerId == 0 ? 1 : brokerId); //TO DO check
        messageStoreConfig.setBrokerRole(BrokerRole.SLAVE);

        //handle the scheduled service
        try {
            messageStore.handleScheduleMessageService(BrokerRole.SLAVE);
        } catch (Throwable t) {
            log.error("[MONITOR] handleScheduleMessageService failed when changing to slave", t);
        }

        //handle the transactional service
        try {
            shutdownProcessorByHa();
        } catch (Throwable t) {
            log.error("[MONITOR] shutdownProcessorByHa failed when changing to slave", t);
        }

        //handle the slave synchronise
        handleSlaveSynchronize(BrokerRole.SLAVE);

        try {
            registerBrokerAll(true, true, brokerConfig.isForceRegister());
        } catch (Throwable ignored) {

        }
        log.info("Finish to change to slave brokerName={} brokerId={}", brokerConfig.getBrokerName(), brokerId);
    }


    public void changeToMaster(BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            return;
        }
        log.info("Begin to change to master brokerName={}", brokerConfig.getBrokerName());

        //handle the slave synchronise
        handleSlaveSynchronize(role);

        //handle the scheduled service
        try {
            messageStore.handleScheduleMessageService(role);
        } catch (Throwable t) {
            log.error("[MONITOR] handleScheduleMessageService failed when changing to master", t);
        }

        //handle the transactional service
        try {
            startProcessorByHa(BrokerRole.SYNC_MASTER);
        } catch (Throwable t) {
            log.error("[MONITOR] startProcessorByHa failed when changing to master", t);
        }

        //if the operations above are totally successful, we change to master
        brokerConfig.setBrokerId(0); //TO DO check
        messageStoreConfig.setBrokerRole(role);

        try {
            registerBrokerAll(true, true, brokerConfig.isForceRegister());
        } catch (Throwable ignored) {

        }
        log.info("Finish to change to master brokerName={}", brokerConfig.getBrokerName());
    }

    private void startProcessorByHa(BrokerRole role) {
        if (BrokerRole.SLAVE != role) {
            if (transactionalMessageCheckService != null) {
                transactionalMessageCheckService.start();
            }
        }
    }

    private void shutdownProcessorByHa() {
        if (transactionalMessageCheckService != null) {
            transactionalMessageCheckService.shutdown(true);
        }
    }

}
