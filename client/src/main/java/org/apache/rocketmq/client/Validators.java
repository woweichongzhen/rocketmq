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

package org.apache.rocketmq.client;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.ResponseCode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 公共校验器
 * Common Validator
 */
public class Validators {
    public static final String VALID_PATTERN_STR = "^[%|a-zA-Z0-9_-]+$";

    /**
     * 只能包含数字字母 %|_-
     */
    public static final Pattern PATTERN = Pattern.compile(VALID_PATTERN_STR);

    /**
     * 组名最大字符长度
     */
    public static final int CHARACTER_MAX_LENGTH = 255;

    /**
     * topic最大字符长度
     */
    public static final int TOPIC_MAX_LENGTH = 127;

    /**
     * @return The resulting {@code String}
     */
    public static String getGroupWithRegularExpression(String origin, String patternStr) {
        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(origin);
        while (matcher.find()) {
            return matcher.group(0);
        }
        return null;
    }

    /**
     * 校验生产者组名
     * Validate group
     */
    public static void checkGroup(String group) throws MQClientException {
        // 不能空白
        if (UtilAll.isBlank(group)) {
            throw new MQClientException("the specified group is blank", null);
        }

        // 最大255字符
        if (group.length() > CHARACTER_MAX_LENGTH) {
            throw new MQClientException("the specified group is longer than group max length 255.", null);
        }

        // 字符校验
        if (!regularExpressionMatcher(group, PATTERN)) {
            throw new MQClientException(String.format(
                    "the specified group[%s] contains illegal characters, allowing only %s", group,
                    VALID_PATTERN_STR), null);
        }

    }

    /**
     * 正则匹配，匹配成功返回true
     *
     * @return <tt>true</tt> if, and only if, the entire origin sequence matches this matcher's pattern
     */
    public static boolean regularExpressionMatcher(String origin, Pattern pattern) {
        if (pattern == null) {
            return true;
        }
        Matcher matcher = pattern.matcher(origin);
        return matcher.matches();
    }

    /**
     * 校验消息
     *
     * @param msg               消息
     * @param defaultMQProducer 生产者
     * @throws MQClientException 客户端异常
     */
    public static void checkMessage(Message msg, DefaultMQProducer defaultMQProducer)
            throws MQClientException {
        // 消息不能为空
        if (null == msg) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message is null");
        }
        // 校验topic
        Validators.checkTopic(msg.getTopic());

        // 校验消息体不能为空
        if (null == msg.getBody()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body is null");
        }

        // 长度不能为0
        if (0 == msg.getBody().length) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body length is zero");
        }

        // 不能超过默认大小，最大4M
        if (msg.getBody().length > defaultMQProducer.getMaxMessageSize()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL,
                    "the message body size over max value, MAX: " + defaultMQProducer.getMaxMessageSize());
        }
    }

    /**
     * 校验topic
     *
     * @param topic topic名称
     * @throws MQClientException mq客户端异常
     */
    public static void checkTopic(String topic) throws MQClientException {
        // 空白
        if (UtilAll.isBlank(topic)) {
            throw new MQClientException("The specified topic is blank", null);
        }

        // 只能为常规字符组成
        if (!regularExpressionMatcher(topic, PATTERN)) {
            throw new MQClientException(String.format(
                    "The specified topic[%s] contains illegal characters, allowing only %s", topic,
                    VALID_PATTERN_STR), null);
        }

        // 最大字符长度127
        if (topic.length() > TOPIC_MAX_LENGTH) {
            throw new MQClientException(
                    String.format("The specified topic is longer than topic max length %d.", TOPIC_MAX_LENGTH), null);
        }

        // 如果为系统保留topic，不允许
        if (topic.equals(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
            throw new MQClientException(
                    String.format("The topic[%s] is conflict with AUTO_CREATE_TOPIC_KEY_TOPIC.", topic), null);
        }
    }

}
