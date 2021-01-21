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

package org.dromara.soul.plugin.sync.data.websocket;

import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dromara.soul.common.concurrent.SoulThreadFactory;
import org.dromara.soul.plugin.sync.data.websocket.client.SoulWebsocketClient;
import org.dromara.soul.plugin.sync.data.websocket.config.WebsocketConfig;
import org.dromara.soul.sync.data.api.AuthDataSubscriber;
import org.dromara.soul.sync.data.api.MetaDataSubscriber;
import org.dromara.soul.sync.data.api.PluginDataSubscriber;
import org.dromara.soul.sync.data.api.SyncDataService;
import org.java_websocket.client.WebSocketClient;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Websocket sync data service.
 *
 * @author xiaoyu(Myth)
 */
@Slf4j
public class WebsocketSyncDataService implements SyncDataService, AutoCloseable {

    private final List<WebSocketClient> clients = new ArrayList<>();

    private final ScheduledThreadPoolExecutor executor;
    
    /**
     * Instantiates a new Websocket sync cache.
     *
     * @param websocketConfig      the websocket config
     * @param pluginDataSubscriber the plugin data subscriber
     * @param metaDataSubscribers  the meta data subscribers
     * @param authDataSubscribers  the auth data subscribers
     */
    public WebsocketSyncDataService(final WebsocketConfig websocketConfig,
                                    final PluginDataSubscriber pluginDataSubscriber,
                                    final List<MetaDataSubscriber> metaDataSubscribers,
                                    final List<AuthDataSubscriber> authDataSubscribers) {
        // 获取soul-admin的地址，多个admin则以,分割
        String[] urls = StringUtils.split(websocketConfig.getUrls(), ",");
        // 采用定时调度的线程池，其核心线程数为admin的地址数，且创建的线程为守护线程
        // 该线程池是为了保证bootstrap与admin之间的websocket能一直连接。
        // 因为如果websocket只是在启动的时候建立连接，在运行过程中发生一些异常导致bootstrap与admin之间的连接断开
        // 那么是有必要去进行重连的，否则只要一断，就失去联系了，不科学
        executor = new ScheduledThreadPoolExecutor(urls.length, SoulThreadFactory.create("websocket-connect", true));
        // 为每一个admin都生成一个websocketClient，与之进行连接
        for (String url : urls) {
            try {
                clients.add(new SoulWebsocketClient(new URI(url), Objects.requireNonNull(pluginDataSubscriber), metaDataSubscribers, authDataSubscribers));
            } catch (URISyntaxException e) {
                log.error("websocket url({}) is error", url, e);
            }
        }
        try {
            // webSocketClient启动连接
            for (WebSocketClient client : clients) {
                boolean success = client.connectBlocking(3000, TimeUnit.MILLISECONDS);
                if (success) {
                    log.info("websocket connection is successful.....");
                } else {
                    log.error("websocket connection is error.....");
                }
                // 与admin之间的websocket定时重连机制
                // 这里是每隔30s就会去检测websocket连接是否还有效
                executor.scheduleAtFixedRate(() -> {
                    try {
                        // 如果client状态为已关闭，则需要重新连接
                        if (client.isClosed()) {
                            boolean reconnectSuccess = client.reconnectBlocking();
                            if (reconnectSuccess) {
                                log.info("websocket reconnect is successful.....");
                            } else {
                                log.error("websocket reconnection is error.....");
                            }
                        }
                    } catch (InterruptedException e) {
                        log.error("websocket connect is error :{}", e.getMessage());
                    }
                }, 10, 30, TimeUnit.SECONDS);
            }
            /* client.setProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("proxyaddress", 80)));*/
        } catch (InterruptedException e) {
            log.info("websocket connection...exception....", e);
        }

    }
    
    @Override
    public void close() {
        for (WebSocketClient client : clients) {
            if (!client.isClosed()) {
                client.close();
            }
        }
        if (Objects.nonNull(executor)) {
            executor.shutdown();
        }
    }
}
