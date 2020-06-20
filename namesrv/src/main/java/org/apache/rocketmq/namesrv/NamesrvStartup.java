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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * 启动类
 */
public class NamesrvStartup {

    /**
     * 日志包装对象
     */
    private static InternalLogger log;

    /**
     * 属性配置
     */
    private static Properties properties = null;

    /**
     * 命令行
     */
    private static CommandLine commandLine = null;

    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {
        try {
            // 创建Controller，核心类
            NamesrvController controller = createNamesrvController(args);
            // 启动
            start(controller);
            // 打印已启动日志，以及目前采用的序列化配置，默认JSON
            String tip =
                    "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * 创建请求处理组件
     *
     * @param args 程序参数
     * @return 处理组件
     * @throws IOException    ioe异常
     * @throws JoranException joran异常
     */
    public static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {
        // 设置rocketmq版本
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();

        // 构建命令行参数：-h 帮助，-n namesrv地址
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        // 解析命令行，如果存在 -h 或解析失败，打印帮助，并退出程序
        commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        // 命令行解析失败，退出
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }

        // namesrv配置类
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        // netty服务器配置类
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        // 设置netty监听端口为9876
        nettyServerConfig.setListenPort(9876);

        // 解析 -c 参数，指定的配置文件路径，并解析属性到配置类中
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                // 加载配置文件到属性中
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);
                // 把属性中的相关值通过反射注入到配置类中
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);
                // 设置加载的配置文件的地址
                namesrvConfig.setConfigStorePath(file);
                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        // 如果存在 -p 参数，获取Console Logger，打印内部配置参数，并退出
        if (commandLine.hasOption('p')) {
            InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_NAME);
            MixAll.printObjectProperties(console, namesrvConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            System.exit(0);
        }

        // 把命令行参数配置写到配置类中
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

        // 如果未配置RocketMQ环境变量，退出
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ " +
                    "installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        // 获取slf4j日志上下文
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        // 配置到logback中
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        // 设置日志配置目录，环境变量主目录/conf/loback_namesrv.xml
        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");

        // 获取内部工厂（缓存 -> slf4j -> inner）的namesrv Logger
        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        // 打印配置类参数到日志文件中
        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);

        // 创建入口类
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

        // 重新记忆所有配置防止被丢弃
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    /**
     * 启动Namesrv入口
     *
     * @param controller 入口
     * @return 启动的入口
     * @throws Exception 异常
     */
    public static NamesrvController start(final NamesrvController controller) throws Exception {
        // 入口为空，异常，启动失败
        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        // 初始化入口，初始化不成功，退出进程
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        // 设置回调钩子，程序关闭时调用入口的关闭
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));

        // 启动入口
        controller.start();

        return controller;
    }

    /**
     * 关闭入口
     *
     * @param controller 入口
     */
    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    /**
     * 构建命令行参数
     *
     * @param options 参数
     * @return 命令行参数
     */
    public static Options buildCommandlineOptions(final Options options) {
        // -c 配置文件，非必须
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        // -p 打印所有配置项，非必须
        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
