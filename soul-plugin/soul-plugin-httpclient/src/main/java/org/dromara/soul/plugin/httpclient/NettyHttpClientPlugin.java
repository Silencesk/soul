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

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import lombok.extern.slf4j.Slf4j;
import org.dromara.soul.common.constant.Constants;
import org.dromara.soul.common.enums.PluginEnum;
import org.dromara.soul.common.enums.RpcTypeEnum;
import org.dromara.soul.plugin.api.SoulPlugin;
import org.dromara.soul.plugin.api.SoulPluginChain;
import org.dromara.soul.plugin.api.context.SoulContext;
import org.dromara.soul.plugin.api.result.SoulResultEnum;
import org.dromara.soul.plugin.base.utils.SoulResultWrap;
import org.dromara.soul.plugin.base.utils.WebFluxResultUtils;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.AbstractServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/**
 * The type Netty http client plugin.
 *
 * @author xiaoyu
 */
@Slf4j
public class NettyHttpClientPlugin implements SoulPlugin {

    private final HttpClient httpClient;

    /**
     * Instantiates a new Netty http client plugin.
     *
     * @param httpClient the http client
     */
    public NettyHttpClientPlugin(final HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Mono<Void> execute(final ServerWebExchange exchange, final SoulPluginChain chain) {
        // 后端服务请求参数的生成
        final SoulContext soulContext = exchange.getAttribute(Constants.CONTEXT);
        assert soulContext != null;
        ServerHttpRequest request = exchange.getRequest();
        final HttpMethod method = HttpMethod.valueOf(request.getMethodValue());
        HttpHeaders filtered = request.getHeaders();
        final DefaultHttpHeaders httpHeaders = new DefaultHttpHeaders();
        filtered.forEach(httpHeaders::set);
        String url = exchange.getAttribute(Constants.HTTP_URL);
        if (StringUtils.isEmpty(url)) {
            Object error = SoulResultWrap.error(SoulResultEnum.CANNOT_FIND_URL.getCode(), SoulResultEnum.CANNOT_FIND_URL.getMsg(), null);
            return WebFluxResultUtils.result(exchange, error);
        }
        log.info("you request, The resulting urlPath is: {}", url);
        Flux<HttpClientResponse> responseFlux =
                // 设置请求头
                this.httpClient.headers(headers -> headers.add(httpHeaders))
                // 构造requestSender
                .request(method).uri(url)
                // 发送请求 nettyOutBound出站的一个处理，即发送请求前的一个处理
                // 需要将请求体的内容转换为netty的ByteBuf传输出去
                .send((req, nettyOutbound) -> nettyOutbound.send(request.getBody().map(dataBuffer -> ((NettyDataBuffer) dataBuffer) .getNativeBuffer())))
                // 将connection提取出Flux<HttpClientResponse>，
                .responseConnection((res, connection) -> {
                    // 将后端服务返回的响应结果放到exchange中
                    exchange.getAttributes().put(Constants.CLIENT_RESPONSE_ATTR, res);
                    // 将connection对象放到exchange中，需要传递给NettyClientResponsePlugin处理
                    exchange.getAttributes().put(Constants.CLIENT_RESPONSE_CONN_ATTR, connection);
                    // 处理header、cookie、httpStatus
                    ServerHttpResponse response = exchange.getResponse();
                    HttpHeaders headers = new HttpHeaders();
                    res.responseHeaders().forEach(entry -> headers.add(entry.getKey(), entry.getValue()));
                    String contentTypeValue = headers.getFirst(HttpHeaders.CONTENT_TYPE);
                    if (StringUtils.hasLength(contentTypeValue)) {
                        exchange.getAttributes().put(Constants.ORIGINAL_RESPONSE_CONTENT_TYPE_ATTR, contentTypeValue);
                    }
                    HttpStatus status = HttpStatus.resolve(res.status().code());
                    if (status != null) {
                        response.setStatusCode(status);
                    } else if (response instanceof AbstractServerHttpResponse) {
                        ((AbstractServerHttpResponse) response)
                                .setStatusCodeValue(res.status().code());
                    } else {
                        throw new IllegalStateException("Unable to set status code on response: " + res.status().code() + ", " + response.getClass());
                    }
                    response.getHeaders().putAll(headers);

                    return Mono.just(res);
                });
        long timeout = (long) Optional.ofNullable(exchange.getAttribute(Constants.HTTP_TIME_OUT)).orElse(3000L);
        Duration duration = Duration.ofMillis(timeout);
        responseFlux = responseFlux
                // 超时
                .timeout(duration, Mono.error(new TimeoutException("Response took longer than timeout: " + duration)))
                // 异常的映射
                .onErrorMap(TimeoutException.class, th -> new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT, th.getMessage(), th));
        // 继续执行插件链
        return responseFlux.then(chain.execute(exchange));
    }

    @Override
    public int getOrder() {
        return PluginEnum.DIVIDE.getCode() + 1;
    }

    @Override
    public Boolean skip(final ServerWebExchange exchange) {
        final SoulContext soulContext = exchange.getAttribute(Constants.CONTEXT);
        assert soulContext != null;
        return !Objects.equals(RpcTypeEnum.HTTP.getName(), soulContext.getRpcType())
                && !Objects.equals(RpcTypeEnum.SPRING_CLOUD.getName(), soulContext.getRpcType());
    }

    @Override
    public String named() {
        return "NettyHttpClient";
    }
}
