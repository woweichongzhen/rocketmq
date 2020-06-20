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

import org.apache.rocketmq.logging.inner.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * 内部logger工厂
 */
public class InnerLoggerFactory extends InternalLoggerFactory {

    public InnerLoggerFactory() {
        doRegister();
    }

    @Override
    protected InternalLogger getLoggerInstance(String name) {
        return new InnerLogger(name);
    }

    @Override
    protected String getLoggerType() {
        return LOGGER_INNER;
    }

    @Override
    protected void shutdown() {
        // 内部logger仓库关闭
        Logger.getRepository().shutdown();
    }

    /**
     * 内部logger的包装类
     */
    public static class InnerLogger implements InternalLogger {

        private final Logger logger;

        public InnerLogger(String name) {
            logger = Logger.getLogger(name);
        }

        @Override
        public String getName() {
            return logger.getName();
        }

        @Override
        public void debug(String var1) {
            logger.debug(var1);
        }

        @Override
        public void debug(String var1, Throwable var2) {
            logger.debug(var1, var2);
        }

        @Override
        public void info(String var1) {
            logger.info(var1);
        }

        @Override
        public void info(String var1, Throwable var2) {
            logger.info(var1, var2);
        }

        @Override
        public void warn(String var1) {
            logger.warn(var1);
        }

        @Override
        public void warn(String var1, Throwable var2) {
            logger.warn(var1, var2);
        }

        @Override
        public void error(String var1) {
            logger.error(var1);
        }

        @Override
        public void error(String var1, Throwable var2) {
            logger.error(var1, var2);
        }

        @Override
        public void debug(String var1, Object var2) {
            // 日志格式化
            FormattingTuple format = MessageFormatter.format(var1, var2);
            logger.debug(format.getMessage(), format.getThrowable());
        }

        @Override
        public void debug(String var1, Object var2, Object var3) {
            FormattingTuple format = MessageFormatter.format(var1, var2, var3);
            logger.debug(format.getMessage(), format.getThrowable());
        }

        @Override
        public void debug(String var1, Object... var2) {
            FormattingTuple format = MessageFormatter.arrayFormat(var1, var2);
            logger.debug(format.getMessage(), format.getThrowable());
        }

        @Override
        public void info(String var1, Object var2) {
            FormattingTuple format = MessageFormatter.format(var1, var2);
            logger.info(format.getMessage(), format.getThrowable());
        }

        @Override
        public void info(String var1, Object var2, Object var3) {
            FormattingTuple format = MessageFormatter.format(var1, var2, var3);
            logger.info(format.getMessage(), format.getThrowable());
        }

        @Override
        public void info(String var1, Object... var2) {
            FormattingTuple format = MessageFormatter.arrayFormat(var1, var2);
            logger.info(format.getMessage(), format.getThrowable());
        }

        @Override
        public void warn(String var1, Object var2) {
            FormattingTuple format = MessageFormatter.format(var1, var2);
            logger.warn(format.getMessage(), format.getThrowable());
        }

        @Override
        public void warn(String var1, Object... var2) {
            FormattingTuple format = MessageFormatter.arrayFormat(var1, var2);
            logger.warn(format.getMessage(), format.getThrowable());
        }

        @Override
        public void warn(String var1, Object var2, Object var3) {
            FormattingTuple format = MessageFormatter.format(var1, var2, var3);
            logger.warn(format.getMessage(), format.getThrowable());
        }

        @Override
        public void error(String var1, Object var2) {
            FormattingTuple format = MessageFormatter.format(var1, var2);
            logger.warn(format.getMessage(), format.getThrowable());
        }

        @Override
        public void error(String var1, Object var2, Object var3) {
            FormattingTuple format = MessageFormatter.format(var1, var2, var3);
            logger.warn(format.getMessage(), format.getThrowable());
        }

        @Override
        public void error(String var1, Object... var2) {
            FormattingTuple format = MessageFormatter.arrayFormat(var1, var2);
            logger.warn(format.getMessage(), format.getThrowable());
        }

        public Logger getLogger() {
            return logger;
        }
    }

    /**
     * 日志格式化后对象
     */
    public static class FormattingTuple {

        /**
         * 信息
         */
        private String message;

        /**
         * 异常
         */
        private Throwable throwable;

        /**
         * 参数数组
         */
        private Object[] argArray;

        public FormattingTuple(String message) {
            this(message, null, null);
        }

        public FormattingTuple(String message, Object[] argArray, Throwable throwable) {
            this.message = message;
            this.throwable = throwable;
            if (throwable == null) {
                this.argArray = argArray;
            } else {
                this.argArray = trimmedCopy(argArray);
            }
        }

        /**
         * 不拷贝异常到参数数组中
         *
         * @param argArray 原参数数组
         * @return 不要异常的参数数组
         */
        static Object[] trimmedCopy(Object[] argArray) {
            if (argArray != null && argArray.length != 0) {
                int trimemdLen = argArray.length - 1;
                Object[] trimmed = new Object[trimemdLen];
                System.arraycopy(argArray, 0, trimmed, 0, trimemdLen);
                return trimmed;
            } else {
                throw new IllegalStateException("non-sensical empty or null argument array");
            }
        }

        public String getMessage() {
            return this.message;
        }

        public Object[] getArgArray() {
            return this.argArray;
        }

        public Throwable getThrowable() {
            return this.throwable;
        }
    }

    /**
     * messge格式化
     */
    public static class MessageFormatter {

        public MessageFormatter() {
        }

        public static FormattingTuple format(String messagePattern, Object arg) {
            return arrayFormat(messagePattern, new Object[]{arg});
        }

        public static FormattingTuple format(String messagePattern, Object arg1, Object arg2) {
            return arrayFormat(messagePattern, new Object[]{arg1, arg2});
        }

        /**
         * 从参数数组中获取最后一个异常参数
         *
         * @param argArray 参数数组
         * @return 异常
         */
        static Throwable getThrowableCandidate(Object[] argArray) {
            if (argArray != null && argArray.length != 0) {
                Object lastEntry = argArray[argArray.length - 1];
                return lastEntry instanceof Throwable ? (Throwable) lastEntry : null;
            } else {
                return null;
            }
        }

        /**
         * 日志格式化
         *
         * @param messagePattern meesage格式
         * @param argArray       参数数组
         * @return 格式化后对象
         */
        public static FormattingTuple arrayFormat(String messagePattern, Object[] argArray) {
            // 获取参数数组中的异常
            Throwable throwableCandidate = getThrowableCandidate(argArray);
            if (messagePattern == null) {
                return new FormattingTuple(null, argArray, throwableCandidate);
            } else if (argArray == null) {
                return new FormattingTuple(messagePattern);
            } else {
                int i = 0;
                StringBuilder sbuf = new StringBuilder(messagePattern.length() + 50);

                int len;
                for (len = 0; len < argArray.length; ++len) {
                    int j = messagePattern.indexOf("{}", i);
                    if (j == -1) {
                        if (i == 0) {
                            return new FormattingTuple(messagePattern, argArray, throwableCandidate);
                        }

                        sbuf.append(messagePattern.substring(i, messagePattern.length()));
                        return new FormattingTuple(sbuf.toString(), argArray, throwableCandidate);
                    }

                    if (isEscapeDelimeter(messagePattern, j)) {
                        if (!isDoubleEscaped(messagePattern, j)) {
                            --len;
                            sbuf.append(messagePattern.substring(i, j - 1));
                            sbuf.append('{');
                            i = j + 1;
                        } else {
                            sbuf.append(messagePattern.substring(i, j - 1));
                            deeplyAppendParameter(sbuf, argArray[len], null);
                            i = j + 2;
                        }
                    } else {
                        sbuf.append(messagePattern.substring(i, j));
                        deeplyAppendParameter(sbuf, argArray[len], null);
                        i = j + 2;
                    }
                }

                sbuf.append(messagePattern.substring(i, messagePattern.length()));
                if (len < argArray.length - 1) {
                    return new FormattingTuple(sbuf.toString(), argArray, throwableCandidate);
                } else {
                    return new FormattingTuple(sbuf.toString(), argArray, null);
                }
            }
        }

        static boolean isEscapeDelimeter(String messagePattern, int delimeterStartIndex) {
            if (delimeterStartIndex == 0) {
                return false;
            } else {
                char potentialEscape = messagePattern.charAt(delimeterStartIndex - 1);
                return potentialEscape == 92;
            }
        }

        static boolean isDoubleEscaped(String messagePattern, int delimeterStartIndex) {
            return delimeterStartIndex >= 2 && messagePattern.charAt(delimeterStartIndex - 2) == 92;
        }

        private static void deeplyAppendParameter(StringBuilder sbuf, Object o, Map<Object[], Object> seenMap) {
            if (o == null) {
                sbuf.append("null");
            } else {
                if (!o.getClass().isArray()) {
                    safeObjectAppend(sbuf, o);
                } else if (o instanceof boolean[]) {
                    booleanArrayAppend(sbuf, (boolean[]) o);
                } else if (o instanceof byte[]) {
                    byteArrayAppend(sbuf, (byte[]) o);
                } else if (o instanceof char[]) {
                    charArrayAppend(sbuf, (char[]) o);
                } else if (o instanceof short[]) {
                    shortArrayAppend(sbuf, (short[]) o);
                } else if (o instanceof int[]) {
                    intArrayAppend(sbuf, (int[]) o);
                } else if (o instanceof long[]) {
                    longArrayAppend(sbuf, (long[]) o);
                } else if (o instanceof float[]) {
                    floatArrayAppend(sbuf, (float[]) o);
                } else if (o instanceof double[]) {
                    doubleArrayAppend(sbuf, (double[]) o);
                } else {
                    objectArrayAppend(sbuf, (Object[]) o, seenMap);
                }

            }
        }

        private static void safeObjectAppend(StringBuilder sbuf, Object o) {
            try {
                String t = o.toString();
                sbuf.append(t);
            } catch (Throwable var3) {
                System.err.println("RocketMQ InnerLogger: Failed toString() invocation on an object of type [" + o.getClass().getName() + "]");
                var3.printStackTrace();
                sbuf.append("[FAILED toString()]");
            }

        }

        private static void objectArrayAppend(StringBuilder sbuf, Object[] a, Map<Object[], Object> seenMap) {
            if (seenMap == null) {
                seenMap = new HashMap<Object[], Object>();
            }
            sbuf.append('[');
            if (!seenMap.containsKey(a)) {
                seenMap.put(a, null);
                int len = a.length;

                for (int i = 0; i < len; ++i) {
                    deeplyAppendParameter(sbuf, a[i], seenMap);
                    if (i != len - 1) {
                        sbuf.append(", ");
                    }
                }

                seenMap.remove(a);
            } else {
                sbuf.append("...");
            }

            sbuf.append(']');
        }

        private static void booleanArrayAppend(StringBuilder sbuf, boolean[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void byteArrayAppend(StringBuilder sbuf, byte[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void charArrayAppend(StringBuilder sbuf, char[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void shortArrayAppend(StringBuilder sbuf, short[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void intArrayAppend(StringBuilder sbuf, int[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void longArrayAppend(StringBuilder sbuf, long[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void floatArrayAppend(StringBuilder sbuf, float[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }

        private static void doubleArrayAppend(StringBuilder sbuf, double[] a) {
            sbuf.append('[');
            int len = a.length;

            for (int i = 0; i < len; ++i) {
                sbuf.append(a[i]);
                if (i != len - 1) {
                    sbuf.append(", ");
                }
            }

            sbuf.append(']');
        }
    }
}
