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

package org.dromara.soul.common.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.dromara.soul.common.constant.Constants;

/**
 * The type Uri utils.
 *
 * @author xiaoyu(Myth)
 */
public class UpstreamCheckUtils {

    private static final Pattern PATTERN = Pattern
            .compile("(http://|https://)?(?:(?:[0,1]?\\d?\\d|2[0-4]\\d|25[0-5])\\.){3}(?:[0,1]?\\d?\\d|2[0-4]\\d|25[0-5]):\\d{0,5}");

    private static final String HTTP = "http";

    /**
     * Check url boolean.
     *
     * @param url the url
     * @return the boolean
     */
    public static boolean checkUrl(final String url) {
        if (StringUtils.isBlank(url)) {
            return false;
        }
        // upstream是否为ip，为ip则用ip+port建立socket连接进行探测
        if (checkIP(url)) {
            String[] hostPort;
            if (url.startsWith(HTTP)) {
                final String[] http = StringUtils.split(url, "\\/\\/");
                hostPort = StringUtils.split(http[1], Constants.COLONS);
            } else {
                hostPort = StringUtils.split(url, Constants.COLONS);
            }
            return isHostConnector(hostPort[0], Integer.parseInt(hostPort[1]));
        } else {
            // 如果不为ip，则直接判断节点host是否可达
            return isHostReachable(url);
        }
    }

    private static boolean checkIP(final String url) {
        return PATTERN.matcher(url).matches();
    }

    private static boolean isHostConnector(final String host, final int port) {
        // 通过ip+port，发起socket连接。如果成功，则表明后端节点存活
        //TODO question 这里没有超时时间设置，如果其中某一个节点连接巨慢，会影响其他节点的探测
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port));
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    private static boolean isHostReachable(final String host) {
        try {
            // ip的探测，根据host获取ip
            return InetAddress.getByName(host).isReachable(1000);
        } catch (IOException ignored) {
        }
        return false;
    }

}
