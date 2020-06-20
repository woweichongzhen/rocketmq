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
package org.apache.rocketmq.client.impl.producer;

import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.CorrelationIdUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 真正的MQ生产者实现类
 */
public class DefaultMQProducerImpl implements MQProducerInner {
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 随机数，生成执行id
     */
    private final Random random = new Random();
    private final DefaultMQProducer defaultMQProducer;

    /**
     * topic发布信息缓存
     * key：topic名称
     * 发布信息缓存
     */
    private final ConcurrentMap<String, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<String,
            TopicPublishInfo>();
    private final ArrayList<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();
    private final RPCHook rpcHook;
    private final BlockingQueue<Runnable> asyncSenderThreadPoolQueue;
    private final ExecutorService defaultAsyncSenderExecutor;
    private final Timer timer = new Timer("RequestHouseKeepingService", true);
    protected BlockingQueue<Runnable> checkRequestQueue;
    protected ExecutorService checkExecutor;

    /**
     * 服务状态
     */
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private ArrayList<CheckForbiddenHook> checkForbiddenHookList = new ArrayList<CheckForbiddenHook>();

    /**
     * zip压缩消息等级，默认为5
     */
    private int zipCompressLevel = Integer.parseInt(System.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "5"));

    /**
     * mq失败策略
     */
    private final MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();
    private ExecutorService asyncSenderExecutor;

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer) {
        this(defaultMQProducer, null);
    }

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer, RPCHook rpcHook) {
        this.defaultMQProducer = defaultMQProducer;
        this.rpcHook = rpcHook;

        this.asyncSenderThreadPoolQueue = new LinkedBlockingQueue<Runnable>(50000);
        this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.asyncSenderThreadPoolQueue,
                new ThreadFactory() {
                    private AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncSenderExecutor_" + this.threadIndex.incrementAndGet());
                    }
                });
    }

    public void registerCheckForbiddenHook(CheckForbiddenHook checkForbiddenHook) {
        this.checkForbiddenHookList.add(checkForbiddenHook);
        log.info("register a new checkForbiddenHook. hookName={}, allHookSize={}", checkForbiddenHook.hookName(),
                checkForbiddenHookList.size());
    }

    public void initTransactionEnv() {
        TransactionMQProducer producer = (TransactionMQProducer) this.defaultMQProducer;
        if (producer.getExecutorService() != null) {
            this.checkExecutor = producer.getExecutorService();
        } else {
            this.checkRequestQueue = new LinkedBlockingQueue<Runnable>(producer.getCheckRequestHoldMax());
            this.checkExecutor = new ThreadPoolExecutor(
                    producer.getCheckThreadPoolMinSize(),
                    producer.getCheckThreadPoolMaxSize(),
                    1000 * 60,
                    TimeUnit.MILLISECONDS,
                    this.checkRequestQueue);
        }
    }

    public void destroyTransactionEnv() {
        if (this.checkExecutor != null) {
            this.checkExecutor.shutdown();
        }
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register sendMessage Hook, {}", hook.hookName());
    }

    /**
     * 启动生产者
     *
     * @throws MQClientException 客户端异常
     */
    public void start() throws MQClientException {
        this.start(true);
    }

    /**
     * 启动生产者
     *
     * @param startFactory 启动工厂
     * @throws MQClientException 客户端异常
     */
    public void start(final boolean startFactory) throws MQClientException {
        switch (serviceState) {
            case CREATE_JUST:
                // 如果仅仅创建
                serviceState = ServiceState.START_FAILED;

                // 检查组名配置
                this.checkConfig();

                // 如果组名为客户端内部生产者组名，改变实例名称为PID
                if (!defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                    defaultMQProducer.changeInstanceNameToPID();
                }

                // 创建客户端实例工厂
                mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(defaultMQProducer, rpcHook);

                // 注册当前实例到工厂中
                boolean registerOK = mQClientFactory.registerProducer(defaultMQProducer.getProducerGroup(), this);
                if (!registerOK) {
                    // 如果注册不成功，服务状态为刚刚创建，抛出异常
                    serviceState = ServiceState.CREATE_JUST;
                    throw new MQClientException("The producer group[" + defaultMQProducer.getProducerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                }

                // 缓存默认用于测试的topic信息
                topicPublishInfoTable.put(defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());

                // 如果启动工厂，启动MQ客户端工厂
                if (startFactory) {
                    mQClientFactory.start();
                }

                log.info("the producer [{}] start OK. sendMessageWithVIPChannel={}",
                        defaultMQProducer.getProducerGroup(),
                        defaultMQProducer.isSendMessageWithVIPChannel());
                // 更改服务状态为运行时
                serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                // 已启动，启动失败，已经关闭的无法再开启
                throw new MQClientException("The producer service state not OK, maybe started once, "
                        + serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
            default:
                break;
        }

        // 发送心跳给所有broker（锁环境下）
        mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        // 启动定时任务，每秒扫描一次过期的请求
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    RequestFutureTable.scanExpiredRequest();
                } catch (Throwable e) {
                    log.error("scan RequestFutureTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }

    /**
     * 检查服务组名配置
     *
     * @throws MQClientException 检查异常
     */
    private void checkConfig() throws MQClientException {
        Validators.checkGroup(defaultMQProducer.getProducerGroup());

        if (null == defaultMQProducer.getProducerGroup()) {
            throw new MQClientException("producerGroup is null", null);
        }

        if (defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
            throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please " +
                    "specify another one.",
                    null);
        }
    }

    public void shutdown() {
        this.shutdown(true);
    }

    public void shutdown(final boolean shutdownFactory) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
                this.defaultAsyncSenderExecutor.shutdown();
                if (shutdownFactory) {
                    this.mQClientFactory.shutdown();
                }

                log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    @Override
    public Set<String> getPublishTopicList() {
        Set<String> topicList = new HashSet<String>();
        for (String key : this.topicPublishInfoTable.keySet()) {
            topicList.add(key);
        }

        return topicList;
    }

    @Override
    public boolean isPublishTopicNeedUpdate(String topic) {
        TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);

        return null == prev || !prev.ok();
    }

    /**
     * This method will be removed in the version 5.0.0 and <code>getCheckListener</code> is recommended.
     *
     * @return
     */
    @Override
    @Deprecated
    public TransactionCheckListener checkListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionCheckListener();
        }

        return null;
    }

    @Override
    public TransactionListener getCheckListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionListener();
        }
        return null;
    }

    @Override
    public void checkTransactionState(final String addr, final MessageExt msg,
                                      final CheckTransactionStateRequestHeader header) {
        Runnable request = new Runnable() {
            private final String brokerAddr = addr;
            private final MessageExt message = msg;
            private final CheckTransactionStateRequestHeader checkRequestHeader = header;
            private final String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGroup();

            @Override
            public void run() {
                TransactionCheckListener transactionCheckListener = DefaultMQProducerImpl.this.checkListener();
                TransactionListener transactionListener = getCheckListener();
                if (transactionCheckListener != null || transactionListener != null) {
                    LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
                    Throwable exception = null;
                    try {
                        if (transactionCheckListener != null) {
                            localTransactionState = transactionCheckListener.checkLocalTransactionState(message);
                        } else if (transactionListener != null) {
                            log.debug("Used new check API in transaction message");
                            localTransactionState = transactionListener.checkLocalTransaction(message);
                        } else {
                            log.warn("CheckTransactionState, pick transactionListener by group[{}] failed", group);
                        }
                    } catch (Throwable e) {
                        log.error("Broker call checkTransactionState, but checkLocalTransactionState exception", e);
                        exception = e;
                    }

                    this.processTransactionState(
                            localTransactionState,
                            group,
                            exception);
                } else {
                    log.warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
                }
            }

            private void processTransactionState(
                    final LocalTransactionState localTransactionState,
                    final String producerGroup,
                    final Throwable exception) {
                final EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
                thisHeader.setCommitLogOffset(checkRequestHeader.getCommitLogOffset());
                thisHeader.setProducerGroup(producerGroup);
                thisHeader.setTranStateTableOffset(checkRequestHeader.getTranStateTableOffset());
                thisHeader.setFromTransactionCheck(true);

                String uniqueKey = message.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                if (uniqueKey == null) {
                    uniqueKey = message.getMsgId();
                }
                thisHeader.setMsgId(uniqueKey);
                thisHeader.setTransactionId(checkRequestHeader.getTransactionId());
                switch (localTransactionState) {
                    case COMMIT_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                        break;
                    case ROLLBACK_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                        log.warn("when broker check, client rollback this transaction, {}", thisHeader);
                        break;
                    case UNKNOW:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                        log.warn("when broker check, client does not know this transaction state, {}", thisHeader);
                        break;
                    default:
                        break;
                }

                String remark = null;
                if (exception != null) {
                    remark = "checkLocalTransactionState Exception: " + RemotingHelper.exceptionSimpleDesc(exception);
                }

                try {
                    DefaultMQProducerImpl.this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr,
                            thisHeader, remark,
                            3000);
                } catch (Exception e) {
                    log.error("endTransactionOneway exception", e);
                }
            }
        };

        this.checkExecutor.submit(request);
    }

    @Override
    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev.toString());
            }
        }
    }

    @Override
    public boolean isUnitMode() {
        return defaultMQProducer.isUnitMode();
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.makeSureStateOK();
        Validators.checkTopic(newTopic);

        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    /**
     * 确认服务状态
     *
     * @throws MQClientException 服务状态不符，抛出异常
     */
    private void makeSureStateOK() throws MQClientException {
        if (serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The producer service state not OK, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
        }
    }

    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public MessageExt viewMessage(
            String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.makeSureStateOK();

        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
            throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    /**
     * DEFAULT ASYNC -------------------------------------------------------
     */
    public void send(Message msg,
                     SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        send(msg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws RejectedExecutionException
     */
    @Deprecated
    public void send(final Message msg, final SendCallback sendCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            sendDefaultImpl(msg, CommunicationMode.ASYNC, sendCallback, timeout - costTime);
                        } catch (Exception e) {
                            sendCallback.onException(e);
                        }
                    } else {
                        sendCallback.onException(
                                new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"));
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }

    }

    /**
     * 选择一个topic的队列
     *
     * @param tpInfo         topic发布信息
     * @param lastBrokerName 上次发送使用的broker名称
     * @return 消息队列
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        return mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation);
    }

    private void validateNameServerSetting() throws MQClientException {
        List<String> nsList = this.getmQClientFactory().getMQClientAPIImpl().getNameServerAddressList();
        if (null == nsList || nsList.isEmpty()) {
            throw new MQClientException(
                    "No name server address, please set it." + FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL), null).setResponseCode(ClientErrorCode.NO_NAME_SERVER_EXCEPTION);
        }

    }

    /**
     * 默认发送消息实现类
     *
     * @param msg               消息
     * @param communicationMode 发送模式
     * @param sendCallback      发送回调
     * @param timeout           超时时间
     * @return 发送结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端异常
     * @throws MQBrokerException    broker异常
     * @throws InterruptedException 中断异常
     */
    private SendResult sendDefaultImpl(Message msg, final CommunicationMode communicationMode,
                                       final SendCallback sendCallback, final long timeout) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException {
        // 确保服务状态为运行中
        this.makeSureStateOK();
        // 校验消息内容
        Validators.checkMessage(msg, defaultMQProducer);

        // 获取执行id
        final long invokeID = random.nextLong();

        // 开始时间
        long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev = beginTimestampFirst;
        long endTimestamp = beginTimestampFirst;

        // 尝试找到topic的发布信息
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            boolean callTimeout = false;
            MessageQueue mq = null;
            Exception exception = null;
            SendResult sendResult = null;
            // 同步发送总次数为 1+重试次数，其他发送为1次
            int timesTotal = communicationMode == CommunicationMode.SYNC
                    ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed()
                    : 1;
            int times = 0;
            // 记录每次发送的broker名称缓存
            String[] brokersSent = new String[timesTotal];
            for (; times < timesTotal; times++) {
                // 上次发送使用的broker名称
                String lastBrokerName = null == mq ? null : mq.getBrokerName();
                // 选择一个队列
                MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
                if (mqSelected != null) {
                    mq = mqSelected;
                    brokersSent[times] = mq.getBrokerName();
                    try {
                        beginTimestampPrev = System.currentTimeMillis();
                        if (times > 0) {
                            // 如果次数大于0，使用命名空间重新包装主题
                            msg.setTopic(defaultMQProducer.withNamespace(msg.getTopic()));
                        }

                        // 扣掉包装花费的时间
                        long costTime = beginTimestampPrev - beginTimestampFirst;
                        if (timeout < costTime) {
                            callTimeout = true;
                            break;
                        }

                        // 发送
                        sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo,
                                timeout - costTime);
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        switch (communicationMode) {
                            case ASYNC:
                                return null;
                            case ONEWAY:
                                return null;
                            case SYNC:
                                // 同步发送，判断发送是否成功，如果需要切换broker重试，继续，否则返回结果
                                if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                    if (defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                                        continue;
                                    }
                                }

                                return sendResult;
                            default:
                                break;
                        }
                    } catch (RemotingException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, " +
                                "Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                    } catch (MQClientException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, " +
                                "Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                    } catch (MQBrokerException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, " +
                                "Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        switch (e.getResponseCode()) {
                            case ResponseCode.TOPIC_NOT_EXIST:
                            case ResponseCode.SERVICE_NOT_AVAILABLE:
                            case ResponseCode.SYSTEM_ERROR:
                            case ResponseCode.NO_PERMISSION:
                            case ResponseCode.NO_BUYER_ID:
                            case ResponseCode.NOT_IN_CURRENT_UNIT:
                                continue;
                            default:
                                if (sendResult != null) {
                                    return sendResult;
                                }

                                throw e;
                        }
                    } catch (InterruptedException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        log.warn(String.format("sendKernelImpl exception, throw exception, InvokeID: %s, RT: %sms, " +
                                "Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());

                        log.warn("sendKernelImpl exception", e);
                        log.warn(msg.toString());
                        throw e;
                    }
                } else {
                    break;
                }
            }

            if (sendResult != null) {
                return sendResult;
            }

            String info = String.format("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: %s",
                    times,
                    System.currentTimeMillis() - beginTimestampFirst,
                    msg.getTopic(),
                    Arrays.toString(brokersSent));

            info += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);

            MQClientException mqClientException = new MQClientException(info, exception);
            if (callTimeout) {
                throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
            }

            if (exception instanceof MQBrokerException) {
                mqClientException.setResponseCode(((MQBrokerException) exception).getResponseCode());
            } else if (exception instanceof RemotingConnectException) {
                mqClientException.setResponseCode(ClientErrorCode.CONNECT_BROKER_EXCEPTION);
            } else if (exception instanceof RemotingTimeoutException) {
                mqClientException.setResponseCode(ClientErrorCode.ACCESS_BROKER_TIMEOUT);
            } else if (exception instanceof MQClientException) {
                mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
            }

            throw mqClientException;
        }

        validateNameServerSetting();

        throw new MQClientException("No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO),
                null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION);
    }

    /**
     * 尝试找到topic的发布信息
     *
     * @param topic topic名称
     * @return topic发布信息
     */
    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        // 如果本地不存在缓存，或者路由信息不可用，从命名服务重新获取路由信息
        TopicPublishInfo topicPublishInfo = topicPublishInfoTable.get(topic);
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
            mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            topicPublishInfo = topicPublishInfoTable.get(topic);
        }

        // 如果仍然没有topic路由信息，再次更新默认topic的信息
        if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
            return topicPublishInfo;
        } else {
            mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, defaultMQProducer);
            topicPublishInfo = topicPublishInfoTable.get(topic);
            return topicPublishInfo;
        }
    }

    /**
     * 核心发送消息
     *
     * @param msg               消息
     * @param mq                队列
     * @param communicationMode 发送模式
     * @param sendCallback      发送回调
     * @param topicPublishInfo  topic发布信息
     * @param timeout           超时时间
     * @return 发送结果
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    private SendResult sendKernelImpl(final Message msg,
                                      final MessageQueue mq,
                                      final CommunicationMode communicationMode,
                                      final SendCallback sendCallback,
                                      final TopicPublishInfo topicPublishInfo,
                                      final long timeout) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        String brokerAddr = mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            // 如果master地址为空，重新获取topic发布信息
            this.tryToFindTopicPublishInfo(mq.getTopic());
            brokerAddr = mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        SendMessageContext context = null;
        if (brokerAddr != null) {
            // 如果不适用VIP通道，则仍使用原来的
            brokerAddr = MixAll.brokerVIPChannel(defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr);

            byte[] prevBody = msg.getBody();
            try {
                //for MessageBatch,ID has been set in the generating process
                // 如果批量消息，设置统一的id
                if (!(msg instanceof MessageBatch)) {
                    MessageClientIDSetter.setUniqID(msg);
                }

                // 设置实例id为命名空间
                boolean topicWithNamespace = false;
                if (null != mQClientFactory.getClientConfig().getNamespace()) {
                    msg.setInstanceId(mQClientFactory.getClientConfig().getNamespace());
                    topicWithNamespace = true;
                }

                // 尝试压缩消息，并添加压缩标记
                int sysFlag = 0;
                boolean msgBodyCompressed = false;
                if (this.tryToCompressMessage(msg)) {
                    sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
                    msgBodyCompressed = true;
                }

                // 如果为事务消息，添加事务标记
                final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (tranMsg != null && Boolean.parseBoolean(tranMsg)) {
                    sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
                }

                // 如果存在检查禁止回调，执行检查禁止回调
                if (this.hasCheckForbiddenHook()) {
                    CheckForbiddenContext checkForbiddenContext = new CheckForbiddenContext();
                    checkForbiddenContext.setNameSrvAddr(defaultMQProducer.getNamesrvAddr());
                    checkForbiddenContext.setGroup(defaultMQProducer.getProducerGroup());
                    checkForbiddenContext.setCommunicationMode(communicationMode);
                    checkForbiddenContext.setBrokerAddr(brokerAddr);
                    checkForbiddenContext.setMessage(msg);
                    checkForbiddenContext.setMq(mq);
                    checkForbiddenContext.setUnitMode(this.isUnitMode());
                    this.executeCheckForbiddenHook(checkForbiddenContext);
                }

                // 如果存在发布消息回调，执行
                if (this.hasSendMessageHook()) {
                    context = new SendMessageContext();
                    context.setProducer(this);
                    context.setProducerGroup(defaultMQProducer.getProducerGroup());
                    context.setCommunicationMode(communicationMode);
                    context.setBornHost(defaultMQProducer.getClientIP());
                    context.setBrokerAddr(brokerAddr);
                    context.setMessage(msg);
                    context.setMq(mq);
                    context.setNamespace(defaultMQProducer.getNamespace());
                    String isTrans = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    if (isTrans != null && "true".equals(isTrans)) {
                        context.setMsgType(MessageType.Trans_Msg_Half);
                    }

                    if (msg.getProperty("__STARTDELIVERTIME") != null
                            || msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null) {
                        context.setMsgType(MessageType.Delay_Msg);
                    }
                    this.executeSendMessageHookBefore(context);
                }

                // 发送消息请求头构建
                SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                requestHeader.setProducerGroup(defaultMQProducer.getProducerGroup());
                requestHeader.setTopic(msg.getTopic());
                requestHeader.setDefaultTopic(defaultMQProducer.getCreateTopicKey());
                requestHeader.setDefaultTopicQueueNums(defaultMQProducer.getDefaultTopicQueueNums());
                requestHeader.setQueueId(mq.getQueueId());
                requestHeader.setSysFlag(sysFlag);
                // 消息出生时间戳
                requestHeader.setBornTimestamp(System.currentTimeMillis());
                requestHeader.setFlag(msg.getFlag());
                // 消息附加属性
                requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
                // 重新消费次数为0
                requestHeader.setReconsumeTimes(0);
                requestHeader.setUnitMode(this.isUnitMode());
                // 是否为批量消息
                requestHeader.setBatch(msg instanceof MessageBatch);

                // 重试组
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    String reconsumeTimes = MessageAccessor.getReconsumeTime(msg);
                    if (reconsumeTimes != null) {
                        // 设置重新消费次数，并清理消息属性
                        requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
                    }

                    // 设置最大重新消费次数，并清理属性
                    String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
                    if (maxReconsumeTimes != null) {
                        requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
                    }
                }

                SendResult sendResult = null;
                switch (communicationMode) {
                    case ASYNC:
                        Message tmpMessage = msg;
                        boolean messageCloned = false;
                        if (msgBodyCompressed) {
                            //If msg body was compressed, msgbody should be reset using prevBody.
                            //Clone new message using commpressed message body and recover origin massage.
                            //Fix bug:https://github.com/apache/rocketmq-externals/issues/66
                            tmpMessage = MessageAccessor.cloneMessage(msg);
                            messageCloned = true;
                            msg.setBody(prevBody);
                        }

                        if (topicWithNamespace) {
                            if (!messageCloned) {
                                tmpMessage = MessageAccessor.cloneMessage(msg);
                                messageCloned = true;
                            }
                            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(),
                                    defaultMQProducer.getNamespace()));
                        }

                        // 异步发送超时
                        long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeAsync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }

                        // 异步发送
                        sendResult = mQClientFactory.getMQClientAPIImpl().sendMessage(
                                brokerAddr,
                                mq.getBrokerName(),
                                tmpMessage,
                                requestHeader,
                                timeout - costTimeAsync,
                                communicationMode,
                                sendCallback,
                                topicPublishInfo,
                                mQClientFactory,
                                defaultMQProducer.getRetryTimesWhenSendAsyncFailed(),
                                context,
                                this);
                        break;
                    case ONEWAY:
                    case SYNC:
                        // 同步发送超时
                        long costTimeSync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeSync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        // 同步发送
                        sendResult = mQClientFactory.getMQClientAPIImpl().sendMessage(
                                brokerAddr,
                                mq.getBrokerName(),
                                msg,
                                requestHeader,
                                timeout - costTimeSync,
                                communicationMode,
                                context,
                                this);
                        break;
                    default:
                        assert false;
                        break;
                }

                // 发送后回调钩子执行
                if (this.hasSendMessageHook()) {
                    context.setSendResult(sendResult);
                    this.executeSendMessageHookAfter(context);
                }

                return sendResult;
            } catch (RemotingException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } catch (MQBrokerException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } catch (InterruptedException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } finally {
                msg.setBody(prevBody);
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), defaultMQProducer.getNamespace()));
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    /**
     * 尝试压缩消息
     *
     * @param msg 消息
     * @return true压缩，false未压缩
     */
    private boolean tryToCompressMessage(final Message msg) {
        // 批量消息不支持压缩
        if (msg instanceof MessageBatch) {
            //batch dose not support compressing right now
            return false;
        }
        byte[] body = msg.getBody();
        if (body != null) {
            // 超过4K，则压缩消息
            if (body.length >= defaultMQProducer.getCompressMsgBodyOverHowmuch()) {
                try {
                    byte[] data = UtilAll.compress(body, zipCompressLevel);
                    if (data != null) {
                        msg.setBody(data);
                        return true;
                    }
                } catch (IOException e) {
                    log.error("tryToCompressMessage exception", e);
                    log.warn(msg.toString());
                }
            }
        }

        return false;
    }

    /**
     * 是否存在检查禁止回调
     *
     * @return true有
     */
    public boolean hasCheckForbiddenHook() {
        return !checkForbiddenHookList.isEmpty();
    }

    public void executeCheckForbiddenHook(final CheckForbiddenContext context) throws MQClientException {
        if (hasCheckForbiddenHook()) {
            for (CheckForbiddenHook hook : checkForbiddenHookList) {
                hook.checkForbidden(context);
            }
        }
    }

    public boolean hasSendMessageHook() {
        return !this.sendMessageHookList.isEmpty();
    }

    public void executeSendMessageHookBefore(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageBefore(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookBefore", e);
                }
            }
        }
    }

    public void executeSendMessageHookAfter(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookAfter", e);
                }
            }
        }
    }

    /**
     * DEFAULT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * KERNEL SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueue mq)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, mq, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueue mq, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        if (!msg.getTopic().equals(mq.getTopic())) {
            throw new MQClientException("message's topic not equal mq's topic", null);
        }

        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeout < costTime) {
            throw new RemotingTooMuchRequestException("call timeout");
        }

        return this.sendKernelImpl(msg, mq, CommunicationMode.SYNC, null, null, timeout);
    }

    /**
     * KERNEL ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        send(msg, mq, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param mq
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        makeSureStateOK();
                        Validators.checkMessage(msg, defaultMQProducer);

                        if (!msg.getTopic().equals(mq.getTopic())) {
                            throw new MQClientException("message's topic not equal mq's topic", null);
                        }
                        long costTime = System.currentTimeMillis() - beginStartTime;
                        if (timeout > costTime) {
                            try {
                                sendKernelImpl(msg, mq, CommunicationMode.ASYNC, sendCallback, null,
                                        timeout - costTime);
                            } catch (MQBrokerException e) {
                                throw new MQClientException("unknown exception", e);
                            }
                        } else {
                            sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                        }
                    } catch (Exception e) {
                        sendCallback.onException(e);
                    }

                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }

    }

    /**
     * KERNEL ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg,
                           MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        try {
            this.sendKernelImpl(msg, mq, CommunicationMode.ONEWAY, null, null,
                    this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * SELECT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, selector, arg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null, timeout);
    }

    private SendResult sendSelectImpl(
            Message msg,
            MessageQueueSelector selector,
            Object arg,
            final CommunicationMode communicationMode,
            final SendCallback sendCallback, final long timeout
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            MessageQueue mq = null;
            try {
                List<MessageQueue> messageQueueList =
                        mQClientFactory.getMQAdminImpl().parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
                Message userMessage = MessageAccessor.cloneMessage(msg);
                String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(),
                        mQClientFactory.getClientConfig().getNamespace());
                userMessage.setTopic(userTopic);

                mq = mQClientFactory.getClientConfig().queueWithNamespace(selector.select(messageQueueList,
                        userMessage, arg));
            } catch (Throwable e) {
                throw new MQClientException("select message queue throwed exception.", e);
            }

            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeout < costTime) {
                throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
            }
            if (mq != null) {
                return this.sendKernelImpl(msg, mq, communicationMode, sendCallback, null, timeout - costTime);
            } else {
                throw new MQClientException("select message queue return null.", null);
            }
        }

        validateNameServerSetting();
        throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
    }

    /**
     * SELECT ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        send(msg, selector, arg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param selector
     * @param arg
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueueSelector selector, final Object arg,
                     final SendCallback sendCallback, final long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            try {
                                sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, sendCallback,
                                        timeout - costTime);
                            } catch (MQBrokerException e) {
                                throw new MQClientException("unknownn exception", e);
                            }
                        } catch (Exception e) {
                            sendCallback.onException(e);
                        }
                    } else {
                        sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("exector rejected ", e);
        }
    }

    /**
     * SELECT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, null,
                    this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    public TransactionSendResult sendMessageInTransaction(final Message msg,
                                                          final LocalTransactionExecuter localTransactionExecuter,
                                                          final Object arg)
            throws MQClientException {
        TransactionListener transactionListener = getCheckListener();
        if (null == localTransactionExecuter && null == transactionListener) {
            throw new MQClientException("tranExecutor is null", null);
        }

        // ignore DelayTimeLevel parameter
        if (msg.getDelayTimeLevel() != 0) {
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        }

        Validators.checkMessage(msg, this.defaultMQProducer);

        SendResult sendResult = null;
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP,
                this.defaultMQProducer.getProducerGroup());
        try {
            sendResult = this.send(msg);
        } catch (Exception e) {
            throw new MQClientException("send message Exception", e);
        }

        LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
        Throwable localException = null;
        switch (sendResult.getSendStatus()) {
            case SEND_OK: {
                try {
                    if (sendResult.getTransactionId() != null) {
                        msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
                    }
                    String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                    if (null != transactionId && !"".equals(transactionId)) {
                        msg.setTransactionId(transactionId);
                    }
                    if (null != localTransactionExecuter) {
                        localTransactionState = localTransactionExecuter.executeLocalTransactionBranch(msg, arg);
                    } else if (transactionListener != null) {
                        log.debug("Used new transaction API");
                        localTransactionState = transactionListener.executeLocalTransaction(msg, arg);
                    }
                    if (null == localTransactionState) {
                        localTransactionState = LocalTransactionState.UNKNOW;
                    }

                    if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE) {
                        log.info("executeLocalTransactionBranch return {}", localTransactionState);
                        log.info(msg.toString());
                    }
                } catch (Throwable e) {
                    log.info("executeLocalTransactionBranch exception", e);
                    log.info(msg.toString());
                    localException = e;
                }
            }
            break;
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                break;
            default:
                break;
        }

        try {
            this.endTransaction(sendResult, localTransactionState, localException);
        } catch (Exception e) {
            log.warn("local transaction execute " + localTransactionState + ", but end broker transaction failed", e);
        }

        TransactionSendResult transactionSendResult = new TransactionSendResult();
        transactionSendResult.setSendStatus(sendResult.getSendStatus());
        transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
        transactionSendResult.setMsgId(sendResult.getMsgId());
        transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
        transactionSendResult.setTransactionId(sendResult.getTransactionId());
        transactionSendResult.setLocalTransactionState(localTransactionState);
        return transactionSendResult;
    }

    /**
     * 默认同步发送消息
     */
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        return this.send(msg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public void endTransaction(
            final SendResult sendResult,
            final LocalTransactionState localTransactionState,
            final Throwable localException) throws RemotingException, MQBrokerException, InterruptedException,
            UnknownHostException {
        final MessageId id;
        if (sendResult.getOffsetMsgId() != null) {
            id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
        } else {
            id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
        }
        String transactionId = sendResult.getTransactionId();
        final String brokerAddr =
                this.mQClientFactory.findBrokerAddressInPublish(sendResult.getMessageQueue().getBrokerName());
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        requestHeader.setTransactionId(transactionId);
        requestHeader.setCommitLogOffset(id.getOffset());
        switch (localTransactionState) {
            case COMMIT_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                break;
            case ROLLBACK_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                break;
            case UNKNOW:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                break;
            default:
                break;
        }

        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
        requestHeader.setMsgId(sendResult.getMsgId());
        String remark = localException != null ?
                ("executeLocalTransactionBranch exception: " + localException.toString()) : null;
        this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, requestHeader, remark,
                this.defaultMQProducer.getSendMsgTimeout());
    }

    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.mQClientFactory.getMQClientAPIImpl().getRemotingClient().setCallbackExecutor(callbackExecutor);
    }

    public ExecutorService getAsyncSenderExecutor() {
        return null == asyncSenderExecutor ? defaultAsyncSenderExecutor : asyncSenderExecutor;
    }

    public void setAsyncSenderExecutor(ExecutorService asyncSenderExecutor) {
        this.asyncSenderExecutor = asyncSenderExecutor;
    }

    /**
     * 同步发送消息
     *
     * @param msg     消息
     * @param timeout 超时时间
     * @return 消息发送结果
     * @throws MQClientException    客户端异常
     * @throws RemotingException    远端异常
     * @throws MQBrokerException    broker异常
     * @throws InterruptedException 中断异常
     */
    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
    }

    public Message request(Message msg,
                           long timeout) throws RequestTimeoutException, MQClientException, RemotingException,
            MQBrokerException, InterruptedException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendReqeustOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendReqeustOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
            if (responseMessage == null) {
                if (requestResponseFuture.isSendRequestOk()) {
                    throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                            "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
                } else {
                    throw new MQClientException("send request message to <" + msg.getTopic() + "> fail",
                            requestResponseFuture.getCause());
                }
            }
            return responseMessage;
        } finally {
            RequestFutureTable.getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(Message msg, final RequestCallback requestCallback, long timeout)
            throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout,
                requestCallback);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendReqeustOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);
    }

    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
                           final long timeout) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendReqeustOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendReqeustOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
            if (responseMessage == null) {
                if (requestResponseFuture.isSendRequestOk()) {
                    throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                            "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
                } else {
                    throw new MQClientException("send request message to <" + msg.getTopic() + "> fail",
                            requestResponseFuture.getCause());
                }
            }
            return responseMessage;
        } finally {
            RequestFutureTable.getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(final Message msg, final MessageQueueSelector selector, final Object arg,
                        final RequestCallback requestCallback, final long timeout)
            throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout,
                requestCallback);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendReqeustOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);

    }

    public Message request(final Message msg, final MessageQueue mq, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException,
            RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendReqeustOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendReqeustOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, null, timeout - cost);

            Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
            if (responseMessage == null) {
                if (requestResponseFuture.isSendRequestOk()) {
                    throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                            "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
                } else {
                    throw new MQClientException("send request message to <" + msg.getTopic() + "> fail",
                            requestResponseFuture.getCause());
                }
            }
            return responseMessage;
        } finally {
            RequestFutureTable.getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
            throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout,
                requestCallback);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendReqeustOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, null, timeout - cost);
    }

    private void requestFail(final String correlationId) {
        RequestResponseFuture responseFuture = RequestFutureTable.getRequestFutureTable().remove(correlationId);
        if (responseFuture != null) {
            responseFuture.setSendReqeustOk(false);
            responseFuture.putResponseMessage(null);
            try {
                responseFuture.executeRequestCallback();
            } catch (Exception e) {
                log.warn("execute requestCallback in requestFail, and callback throw", e);
            }
        }
    }

    private void prepareSendRequest(final Message msg, long timeout) {
        String correlationId = CorrelationIdUtil.createCorrelationId();
        String requestClientId = this.getmQClientFactory().getClientId();
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));

        boolean hasRouteData = this.getmQClientFactory().getTopicRouteTable().containsKey(msg.getTopic());
        if (!hasRouteData) {
            long beginTimestamp = System.currentTimeMillis();
            this.tryToFindTopicPublishInfo(msg.getTopic());
            this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
            long cost = System.currentTimeMillis() - beginTimestamp;
            if (cost > 500) {
                log.warn("prepare send request for <{}> cost {} ms", msg.getTopic(), cost);
            }
        }
    }

    public ConcurrentMap<String, TopicPublishInfo> getTopicPublishInfoTable() {
        return topicPublishInfoTable;
    }

    public int getZipCompressLevel() {
        return zipCompressLevel;
    }

    public void setZipCompressLevel(int zipCompressLevel) {
        this.zipCompressLevel = zipCompressLevel;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public long[] getNotAvailableDuration() {
        return this.mqFaultStrategy.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.mqFaultStrategy.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.mqFaultStrategy.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.mqFaultStrategy.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.mqFaultStrategy.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.mqFaultStrategy.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }
}
