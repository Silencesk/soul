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

package org.dromara.soul.admin.listener.websocket;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dromara.soul.admin.service.SyncDataService;
import org.dromara.soul.admin.spring.SpringBeanUtils;
import org.dromara.soul.common.enums.DataEventTypeEnum;

import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * The type Websocket data changed listener.
 *
 * @author xiaoyu(Myth)
 * @author huangxiaofeng
 * @since 2.0.0
 */
@Slf4j
@ServerEndpoint("/websocket")
public class WebsocketCollector {

    /**
     * CopyOnWriteArraySet可理解为HashSet的线程安全版本
     * 底层通过 CopyOnWriteArrayList 实现
     * 适用于Set大小通常保持很小，只读操作远多于写操作，需要在遍历期间防止线程间的冲突的场景
     */
    private static final Set<Session> SESSION_SET = new CopyOnWriteArraySet<>();

    private static Session session;

    /**
     * On open.
     *
     * @param session the session
     */
    @OnOpen
    public void onOpen(final Session session) {
        log.info("websocket on open successful....");
        SESSION_SET.add(session);
    }

    /**
     * On message.
     *
     * @param message the message
     * @param session the session
     */
    @OnMessage
    public void onMessage(final String message, final Session session) {
        // 收到myself的消息，用于bootstrap初始连接上来的时候，相当于是给admin自己的消息
        // 该消息会同步所有类型的配置数据，给到连上来的这个bootstrap
        // 如果有多台bootstrap同时连接上来，又会怎样？onMessage保证了有序性？这里应该存在并发问题
        // TODO question
        if (message.equals(DataEventTypeEnum.MYSELF.name())) {
            WebsocketCollector.session = session;
            SpringBeanUtils.getInstance().getBean(SyncDataService.class).syncAll(DataEventTypeEnum.MYSELF);
        }
    }

    /**
     * On close.
     *
     * @param session the session
     */
    @OnClose
    public void onClose(final Session session) {
        SESSION_SET.remove(session);
        WebsocketCollector.session = null;
    }

    /**
     * On error.
     *
     * @param session the session
     * @param error   the error
     */
    @OnError
    public void onError(final Session session, final Throwable error) {
        SESSION_SET.remove(session);
        WebsocketCollector.session = null;
        log.error("websocket collection error: ", error);
    }

    /**
     * Send.
     *
     * @param message the message
     * @param type    the type
     */
    public static void send(final String message, final DataEventTypeEnum type) {
        if (StringUtils.isNotBlank(message)) {
            // 用自身的消息类型Myself代表所有配置类型数据
            if (DataEventTypeEnum.MYSELF == type) {
                try {
                    session.getBasicRemote().sendText(message);
                } catch (IOException e) {
                    log.error("websocket send result is exception: ", e);
                }
                return;
            }
            // 其他消息则遍历所有的websocket请求，依次发送消息；这里有多个session，因soul-admin可能是集群
            for (Session session : SESSION_SET) {
                try {
                    session.getBasicRemote().sendText(message);
                } catch (IOException e) {
                    log.error("websocket send result is exception: ", e);
                }
            }
        }
    }
}
