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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;

/**
 * broker启动
 */
public class BrokerStartup {

    /**
     * 属性
     */
    public static Properties properties = null;

    /**
     * 命令行
     */
    public static CommandLine commandLine = null;

    /**
     * 配置文件
     */
    public static String configFile = null;

    /**
     * 内部log
     */
    public static InternalLogger log;

    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    /**
     * 启动入口
     *
     * @param controller 入口
     * @return broker入口
     */
    public static BrokerController start(BrokerController controller) {
        try {
            // 启动入口
            controller.start();

            String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                    + controller.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            // 打印broker相关日志，以及序列化配置（JSON）
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            // 启动失败，退出进程
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * 关闭入口
     *
     * @param controller 入口
     */
    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    /**
     * 创建broker入口
     *
     * @param args 参数
     * @return broker入口
     */
    public static BrokerController createBrokerController(String[] args) {
        // 设置rocket版本
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        // socket发送缓冲区大小
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            NettySystemConfig.socketSndbufSize = 131072;
        }

        // socket接收缓存区大小
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            NettySystemConfig.socketRcvbufSize = 131072;
        }

        try {
            //PackageConflictDetect.detectFastjson();
            // 构建命令行，如果不存在，退出
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
                    new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
            }

            // broker核心配置，netty服务配置，netty客户配置
            final BrokerConfig brokerConfig = new BrokerConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();

            // 设置是否启用TLS加密传输
            nettyClientConfig.setUseTLS(Boolean.parseBoolean(System.getProperty(TLS_ENABLE,
                    String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING))));
            // 设置broker监听端口，默认10911
            nettyServerConfig.setListenPort(10911);

            // 消息存储配置
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
            // 如果是 SLAVE 节点，设置可访问消息的内存最大比例
            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }

            // 如果是 -c 参数，导入配置文件中配置，设置到broker配置，netty服务，客户端配置，消息存储配置中
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    // 把配置文件的部分域名属性转换设置到环境变量中
                    properties2SystemEnv(properties);
                    MixAll.properties2Object(properties, brokerConfig);
                    MixAll.properties2Object(properties, nettyServerConfig);
                    MixAll.properties2Object(properties, nettyClientConfig);
                    MixAll.properties2Object(properties, messageStoreConfig);

                    // 并设置文件路径到broker路径配置辅助类中
                    BrokerPathConfigHelper.setBrokerConfigPath(file);
                    in.close();
                }
            }

            // 注入所有命令行参数到 broker 配置类中
            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

            // 如果RocketMQ主目录未配置，异常退出
            if (null == brokerConfig.getRocketmqHome()) {
                System.out.printf("Please set the %s variable in your environment to match the location of the " +
                        "RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
                System.exit(-2);
            }

            // 获取namesrv服务地址，解析，判断格式是否有误
            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {
                try {
                    String[] addrArray = namesrvAddr.split(";");
                    for (String addr : addrArray) {
                        RemotingUtil.string2SocketAddress(addr);
                    }
                } catch (Exception e) {
                    System.out.printf(
                            "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168" +
                                    ".0.1:9876\"%n",
                            namesrvAddr);
                    System.exit(-3);
                }
            }

            // 判断broker角色，如果同步或异步同步的，就是masterId，即0
            // 如果为SLAVE角色，并且brokerId配置不符，也就是 <= 0，退出
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= 0) {
                        System.out.printf("Slave's brokerId must be > 0");
                        System.exit(-3);
                    }

                    break;
                default:
                    break;
            }

            // 是否启用基于 DLeger 技术来管理 Broker 的 CommitLog ，如果使用其管理，设置 brokerId 为 -1
            // 也就是角色被全面托管了
            if (messageStoreConfig.isEnableDLegerCommitLog()) {
                brokerConfig.setBrokerId(-1);
            }

            // 设置消息存储的高可用监听端口，即主从同步端口，为broker端口号+1
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);

            // 设置slf4j 日志配置
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");

            // 如果存在 -p 参数，获取broker控制台logger，打印各个配置参数，并退出
            if (commandLine.hasOption('p')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig);
                MixAll.printObjectProperties(console, nettyServerConfig);
                MixAll.printObjectProperties(console, nettyClientConfig);
                MixAll.printObjectProperties(console, messageStoreConfig);
                System.exit(0);
            } else if (commandLine.hasOption('m')) {
                // 如果存在 -m 参数，获取broker控制台logger，仅打印注解注入的配置
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig, true);
                MixAll.printObjectProperties(console, nettyServerConfig, true);
                MixAll.printObjectProperties(console, nettyClientConfig, true);
                MixAll.printObjectProperties(console, messageStoreConfig, true);
                System.exit(0);
            }

            // 获取内部真正的Logger，并打印所有配置到日志文件中
            log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
            MixAll.printObjectProperties(log, brokerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            MixAll.printObjectProperties(log, nettyClientConfig);
            MixAll.printObjectProperties(log, messageStoreConfig);

            // 创建broker入口
            final BrokerController controller = new BrokerController(
                    brokerConfig,
                    nettyServerConfig,
                    nettyClientConfig,
                    messageStoreConfig);
            // 记忆所有配置项，防止丢失
            controller.getConfiguration().registerConfig(properties);

            // 初始化broker，初始化失败关闭入口，并退出
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            // 注册回调钩子，同步任务
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

                /**
                 * 是否关闭
                 */
                private volatile boolean hasShutdown = false;

                /**
                 * 关闭次数
                 */
                private final AtomicInteger shutdownTimes = new AtomicInteger(0);

                @Override
                public void run() {
                    synchronized (this) {
                        // 打印执行关闭次数
                        log.info("Shutdown hook was invoked, {}", shutdownTimes.incrementAndGet());
                        // 如果还没有关闭，更新变量为已关闭，并关闭入口，打印关闭入口耗时
                        if (!hasShutdown) {
                            hasShutdown = true;
                            long beginTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return controller;
        } catch (Throwable e) {
            // 初始化异常，退出进行
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * 把配置文件的部分域名属性转换设置到环境变量中
     *
     * @param properties 属性
     */
    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        // namesrv服务域名
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        // namesrv域名子组
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    /**
     * 构建命令行参数
     *
     * @param options 参数
     * @return 命令行允许参数
     */
    private static Options buildCommandlineOptions(final Options options) {
        // -c 指定broker配置文件，非必须
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        // -p 打印所有配置项，非必须
        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        // -m 打印导入配置文件项，非必须
        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
