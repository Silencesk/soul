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

package org.dromara.soul.plugin.divide.balance.spi;

import org.dromara.soul.common.dto.convert.DivideUpstream;
import org.dromara.soul.spi.Join;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round robin load balance impl.
 *
 * @author xiaoyu
 */
@Join
public class RoundRobinLoadBalance extends AbstractLoadBalance {

    /**
     * 重置间隔60s
     */
    private final int recyclePeriod = 60000;

    /**
     * key -> 后端server，为ip:port   value -> 同属于服务列表的所有后端服务的权重对象
     */
    private final ConcurrentMap<String, ConcurrentMap<String, WeightedRoundRobin>> methodWeightMap = new ConcurrentHashMap<>(16);

    private final AtomicBoolean updateLock = new AtomicBoolean();

    @Override
    public DivideUpstream doSelect(final List<DivideUpstream> upstreamList, final String ip) {
        //TODO question 如果这批upstream第一个节点经常变更，可能会导致前面节点生成的WeightedRoundRobin数据无法释放
        String key = upstreamList.get(0).getUpstreamUrl();
        // 取出此批后端服务的权重对象 key -> 后端server，为ip:port   value -> 后端server对应的权重对象
        ConcurrentMap<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map == null) {
            methodWeightMap.putIfAbsent(key, new ConcurrentHashMap<>(16));
            map = methodWeightMap.get(key);
        }
        // 总权重值
        int totalWeight = 0;
        // 当前请求权重的最大值，每次请求都会置为MIN_VALUE
        long maxCurrent = Long.MIN_VALUE;
        // 当前系统时间
        long now = System.currentTimeMillis();
        // 选中节点
        DivideUpstream selectedInvoker = null;
        // 选中节点的权重
        WeightedRoundRobin selectedWRR = null;
        //TODO question 轮询这里为什么没有break，应该找到选中节点后就跳出循环，不需要继续循环下去
        for (DivideUpstream upstream : upstreamList) {
            // rkey -> 后端server，ip:port
            String rKey = upstream.getUpstreamUrl();
            // 后端server对应的权重对象
            WeightedRoundRobin weightedRoundRobin = map.get(rKey);
            // 当前后端节点的权重值
            int weight = getWeight(upstream);
            // 如果当前后端server权重对象不存在，则需生成当前后端server的权重对象，并添加到内存
            if (weightedRoundRobin == null) {
                weightedRoundRobin = new WeightedRoundRobin();
                weightedRoundRobin.setWeight(weight);
                map.putIfAbsent(rKey, weightedRoundRobin);
            }
            // 节点权重值发生了变化，则需要重新设置
            if (weight != weightedRoundRobin.getWeight()) {
                //weight changed
                // 权重值发生变化后，将内存中后端server权重更新为最新权重值
                // 同时其内部current值也将更新为0，回到初始状态
                weightedRoundRobin.setWeight(weight);
            }
            // 内部序列从0开始增，每次进来都+自己的权重值
            long cur = weightedRoundRobin.increaseCurrent();
            // 重新设置更新时间
            weightedRoundRobin.setLastUpdate(now);
            // 如果节点当前轮询权重大于此次调用的最大权重值，则满足项，可以作为选中节点
            // 这里选中节点后，并不会停止循环，会遍历所有的节点，将最后一个满足条件的节点作为选中节点
            // 这里是为了找到当前权重最大的那个后端server，将其作为选中节点
            if (cur > maxCurrent) {
                maxCurrent = cur;
                selectedInvoker = upstream;
                selectedWRR = weightedRoundRobin;
            }
            // 总权重
            totalWeight += weight;
        }
        // 更新标志锁位未更新  后端节点数不等于原有节点数  采用compareAndSet取锁，获取到锁之后才更新
        if (!updateLock.get() && upstreamList.size() != map.size() && updateLock.compareAndSet(false, true)) {
            try {
                // copy -> modify -> update reference
                // 新的map
                ConcurrentMap<String, WeightedRoundRobin> newMap = new ConcurrentHashMap<>(map);
                // 如果从轮询开始，到结束，中间间隔了1min钟，则需要移除该后端server的权重对象数据
                // 这里是为了移除掉那些下线的节点，因为只要有被轮询，其LastUpdate=now；而超过1min钟，都没有轮询到的，则就是已下线的节点
                newMap.entrySet().removeIf(item -> now - item.getValue().getLastUpdate() > recyclePeriod);
                // 将新的后端节点的权重数据替换旧有的
                methodWeightMap.put(key, newMap);
            } finally {
                // 最终将锁的标志为设置为false
                updateLock.set(false);
            }
        }
        // 选中节点的处理
        if (selectedInvoker != null) {
            // 选中节点的内部计数的当前权重current要减去总的权重值，降低其当前权重，也就意味着其下次被选中的优先级被降低
            selectedWRR.sel(totalWeight);
            return selectedInvoker;
        }
        // should not happen here
        return upstreamList.get(0);
    }

    /**
     * The type Weighted round robin.
     */
    protected static class WeightedRoundRobin {

        private int weight;

        private final AtomicLong current = new AtomicLong(0);

        private long lastUpdate;

        /**
         * Gets weight.
         *
         * @return the weight
         */
        int getWeight() {
            return weight;
        }

        /**
         * Sets weight.
         *
         * @param weight the weight
         */
        void setWeight(final int weight) {
            this.weight = weight;
            current.set(0);
        }

        /**
         * Increase current long.
         *
         * @return the long
         */
        long increaseCurrent() {
            return current.addAndGet(weight);
        }

        /**
         * Sel.
         *
         * @param total the total
         */
        void sel(final int total) {
            current.addAndGet(-1 * total);
        }

        /**
         * Gets last update.
         *
         * @return the last update
         */
        long getLastUpdate() {
            return lastUpdate;
        }

        /**
         * Sets last update.
         *
         * @param lastUpdate the last update
         */
        void setLastUpdate(final long lastUpdate) {
            this.lastUpdate = lastUpdate;
        }
    }

}
