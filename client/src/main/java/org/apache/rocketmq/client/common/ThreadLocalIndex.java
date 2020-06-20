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

package org.apache.rocketmq.client.common;

import java.util.Random;

/**
 * 线程计数器
 */
public class ThreadLocalIndex {

    /**
     * 线程计数器
     */
    private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal<Integer>();

    /**
     * 随机值
     */
    private final Random random = new Random();

    /**
     * 获取并自增
     *
     * @return 自增前的
     */
    public int getAndIncrement() {
        Integer index = threadLocalIndex.get();
        // 如果为空，随机值，如果随机小于0，从0开始
        if (null == index) {
            index = Math.abs(random.nextInt());
            if (index < 0) {
                index = 0;
            }
            threadLocalIndex.set(index);
        }

        index = Math.abs(index + 1);
        if (index < 0) {
            index = 0;
        }

        threadLocalIndex.set(index);
        return index;
    }

    @Override
    public String toString() {
        return "ThreadLocalIndex{" +
                "threadLocalIndex=" + threadLocalIndex.get() +
                '}';
    }
}
