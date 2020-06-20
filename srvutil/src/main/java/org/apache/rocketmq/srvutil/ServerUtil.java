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
package org.apache.rocketmq.srvutil;

import org.apache.commons.cli.*;

import java.util.Properties;

/**
 * srv工具类
 */
public class ServerUtil {

    /**
     * 构建命令行选项
     *
     * @param options 选项
     * @return 命令行选项
     */
    public static Options buildCommandlineOptions(final Options options) {
        // 帮助
        // 短参数，长参数，描述，非必须
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        // namesrv地址，非必须
        opt = new Option("n", "namesrvAddr", true,
                "Name server address list, eg: 192.168.0.1:9876;192.168.0.2:9876");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    /**
     * 解析cmd命令行
     *
     * @param appName 应用名称
     * @param args    命令行参数
     * @param options 选项
     * @param parser  命令行解析器
     * @return 命令行
     */
    public static CommandLine parseCmdLine(final String appName, String[] args, Options options,
                                           CommandLineParser parser) {
        // 格式化辅助类
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
            // 如果存在 -h，打印相关帮助，退出程序
            if (commandLine.hasOption('h')) {
                hf.printHelp(appName, options, true);
                System.exit(0);
            }
        } catch (ParseException e) {
            // 解析异常，打印帮助，退出程序
            hf.printHelp(appName, options, true);
            System.exit(1);
        }

        return commandLine;
    }

    public static void printCommandLineHelp(final String appName, final Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(appName, options, true);
    }

    /**
     * 命令行转为属性
     *
     * @param commandLine 命令行
     * @return 属性
     */
    public static Properties commandLine2Properties(final CommandLine commandLine) {
        Properties properties = new Properties();
        Option[] opts = commandLine.getOptions();

        // 遍历命令行参数，设置长参数和value到属性中
        if (opts != null) {
            for (Option opt : opts) {
                String name = opt.getLongOpt();
                String value = commandLine.getOptionValue(name);
                if (value != null) {
                    properties.setProperty(name, value);
                }
            }
        }

        return properties;
    }

}
