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

package org.dromara.soul.plugin.httpclient;

import io.netty.channel.ConnectTimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dromara.soul.common.constant.Constants;
import org.dromara.soul.common.enums.PluginEnum;
import org.dromara.soul.common.enums.ResultEnum;
import org.dromara.soul.common.enums.RpcTypeEnum;
import org.dromara.soul.plugin.api.SoulPlugin;
import org.dromara.soul.plugin.api.SoulPluginChain;
import org.dromara.soul.plugin.api.context.SoulContext;
import org.dromara.soul.plugin.api.result.SoulResultEnum;
import org.dromara.soul.plugin.base.utils.SoulResultWrap;
import org.dromara.soul.plugin.base.utils.WebFluxResultUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.retry.Backoff;
import reactor.retry.Retry;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The type Web client plugin.
 *
 * @author xiaoyu
 */
@Slf4j
public class WebClientPlugin implements SoulPlugin {

    private final WebClient webClient;

    /**
     * Instantiates a new Web client plugin.
     *
     * @param webClient the web client
     */
    public WebClientPlugin(final WebClient webClient) {
        this.webClient = webClient;
    }

    @Override
    public Mono<Void> execute(final ServerWebExchange exchange, final SoulPluginChain chain) {
        // 取出网关上下文
        final SoulContext soulContext = exchange.getAttribute(Constants.CONTEXT);
        assert soulContext != null;
        // 请求路径
        String urlPath = exchange.getAttribute(Constants.HTTP_URL);
        if (StringUtils.isEmpty(urlPath)) {
            Object error = SoulResultWrap.error(SoulResultEnum.CANNOT_FIND_URL.getCode(), SoulResultEnum.CANNOT_FIND_URL.getMsg(), null);
            return WebFluxResultUtils.result(exchange, error);
        }
        long timeout = (long) Optional.ofNullable(exchange.getAttribute(Constants.HTTP_TIME_OUT)).orElse(3000L);
        int retryTimes = (int) Optional.ofNullable(exchange.getAttribute(Constants.HTTP_RETRY)).orElse(0);
        log.info("The request urlPath is {}, retryTimes is {}", urlPath, retryTimes);
        // 原始请求的方法类型
        HttpMethod method = HttpMethod.valueOf(exchange.getRequest().getMethodValue());
        // 请求体的构造
        WebClient.RequestBodySpec requestBodySpec = webClient.method(method).uri(urlPath);
        // 处理请求体以及发送请求
        return handleRequestBody(requestBodySpec, exchange, timeout, retryTimes, chain);
    }

    @Override
    public int getOrder() {
        return PluginEnum.DIVIDE.getCode() + 1;
    }

    @Override
    public String named() {
        return "webClient";
    }

    @Override
    public Boolean skip(final ServerWebExchange exchange) {
        final SoulContext soulContext = exchange.getAttribute(Constants.CONTEXT);
        assert soulContext != null;
        return !Objects.equals(RpcTypeEnum.HTTP.getName(), soulContext.getRpcType())
                && !Objects.equals(RpcTypeEnum.SPRING_CLOUD.getName(), soulContext.getRpcType());
    }

    private MediaType buildMediaType(final ServerWebExchange exchange) {
        return MediaType.valueOf(Optional.ofNullable(exchange
                .getRequest()
                .getHeaders().getFirst(HttpHeaders.CONTENT_TYPE))
                .orElse(MediaType.APPLICATION_JSON_VALUE));
    }

    private Mono<Void> handleRequestBody(final WebClient.RequestBodySpec requestBodySpec,
                                         final ServerWebExchange exchange,
                                         final long timeout,
                                         final int retryTimes,
                                         final SoulPluginChain chain) {
        return requestBodySpec
                // 请求头
                .headers(httpHeaders -> {
                    httpHeaders.addAll(exchange.getRequest().getHeaders());
                    httpHeaders.remove(HttpHeaders.HOST);
                })
                // 请求内容的类型
                .contentType(buildMediaType(exchange))
                // 请求body
                .body(BodyInserters.fromDataBuffers(exchange.getRequest().getBody()))
                // 执行请求
                .exchange()
                // 请求异常时的处理
                .doOnError(e -> log.error(e.getMessage()))
                // 超时异常的抛出
                .timeout(Duration.ofMillis(timeout))
                // 重试：只有当请求发生连接超时，重试次数，重试算法使用2的指数退让，第一次重试等待200ms，期间最大间隔为20s
                .retryWhen(Retry.onlyIf(x -> x.exception() instanceof ConnectTimeoutException)
                        .retryMax(retryTimes)
                        .backoff(Backoff.exponential(Duration.ofMillis(200), Duration.ofSeconds(20), 2, true)))
                // 将flux -> mono返回
                .flatMap(e -> doNext(e, exchange, chain));

    }

    private Mono<Void> doNext(final ClientResponse res, final ServerWebExchange exchange, final SoulPluginChain chain) {
        if (res.statusCode().is2xxSuccessful()) {
            exchange.getAttributes().put(Constants.CLIENT_RESPONSE_RESULT_TYPE, ResultEnum.SUCCESS.getName());
        } else {
            exchange.getAttributes().put(Constants.CLIENT_RESPONSE_RESULT_TYPE, ResultEnum.ERROR.getName());
        }
        // 将后端节点返回的响应放到exchange中
        exchange.getAttributes().put(Constants.CLIENT_RESPONSE_ATTR, res);
        // 继续执行soul插件链
        return chain.execute(exchange);
    }
}
