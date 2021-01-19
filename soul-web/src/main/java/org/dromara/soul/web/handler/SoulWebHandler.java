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

package org.dromara.soul.web.handler;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.dromara.soul.metrics.api.HistogramMetricsTrackerDelegate;
import org.dromara.soul.metrics.enums.MetricsLabelEnum;
import org.dromara.soul.metrics.facade.MetricsTrackerFacade;
import org.dromara.soul.plugin.api.SoulPlugin;
import org.dromara.soul.plugin.api.SoulPluginChain;
import org.springframework.lang.NonNull;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebHandler;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * This is web handler request starter.
 *
 * @author xiaoyu(Myth)
 */
public final class SoulWebHandler implements WebHandler {

    private final List<SoulPlugin> plugins;

    private final Scheduler scheduler;

    /**
     * Instantiates a new Soul web handler.
     *
     * @param plugins the plugins
     */
    public SoulWebHandler(final List<SoulPlugin> plugins) {
        this.plugins = plugins;
        String schedulerType = System.getProperty("soul.scheduler.type", "fixed");
        if (Objects.equals(schedulerType, "fixed")) {
            int threads = Integer.parseInt(System.getProperty(
                    "soul.work.threads", "" + Math.max((Runtime.getRuntime().availableProcessors() << 1) + 1, 16)));
            scheduler = Schedulers.newParallel("soul-work-threads", threads);
        } else {
            scheduler = Schedulers.elastic();
        }
    }

    /**
     * Handle the web server exchange.
     *
     * @param exchange the current server exchange
     * @return {@code Mono<Void>} to indicate when request handling is complete
     */
    @Override
    public Mono<Void> handle(@NonNull final ServerWebExchange exchange) {
        MetricsTrackerFacade.getInstance().counterInc(MetricsLabelEnum.REQUEST_TOTAL.getName());
        Optional<HistogramMetricsTrackerDelegate> startTimer = MetricsTrackerFacade.getInstance().histogramStartTimer(MetricsLabelEnum.REQUEST_LATENCY.getName());
        // 直接将插件列表生成一个插件链DefaultSoulPluginChain，责任链的execute方法会遍历所有插件列表，依次处理
        // Mono的subscribeOn没看明白？？？【就是讲线程池绑定到Mono请求流程中】；难道是经过Mono的请求，都会提交给这个线程池执行？Mono本身是有默认线程池吧，这里为什么要自用线程池？应该是自行创建的Mono对象
        return new DefaultSoulPluginChain(plugins).execute(exchange).subscribeOn(scheduler)
                .doOnSuccess(t -> startTimer.ifPresent(time -> MetricsTrackerFacade.getInstance().histogramObserveDuration(time)));
    }

    private static class DefaultSoulPluginChain implements SoulPluginChain {

        private int index;

        private final List<SoulPlugin> plugins;

        /**
         * Instantiates a new Default soul plugin chain.
         *
         * @param plugins the plugins
         */
        DefaultSoulPluginChain(final List<SoulPlugin> plugins) {
            this.plugins = plugins;
        }

        /**
         * Delegate to the next {@code WebFilter} in the chain.
         *
         * @param exchange the current server exchange
         * @return {@code Mono<Void>} to indicate when request handling is complete
         */
        @Override
        // 1. 执行方法，遍历插件列表，并调用插件的执行逻辑
        // 2. 返回Mono对象，能方便的使用Reactive的事件驱动编程的机制
        public Mono<Void> execute(final ServerWebExchange exchange) {
            return Mono.defer(() -> {
                if (this.index < plugins.size()) {
                    SoulPlugin plugin = plugins.get(this.index++);
                    // 先判断当前请求是否满足插件处理的逻辑，如果不满足，则跳过不处理；否则进行处理
                    Boolean skip = plugin.skip(exchange);
                    if (skip) {
                        // 跳过，直接执行下一个插件
                        return this.execute(exchange);
                    }
                    // 执行当前插件的逻辑
                    return plugin.execute(exchange, this);
                }
                // 所有插件执行完毕后，返回空的Mono对象
                return Mono.empty();
            });
        }
    }
}
