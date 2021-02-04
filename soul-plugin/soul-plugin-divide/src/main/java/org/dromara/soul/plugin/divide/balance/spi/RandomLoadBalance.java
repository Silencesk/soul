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
import java.util.Random;

/**
 * random algorithm impl.
 *
 * @author xiaoyu(Myth)
 */
@Join
public class RandomLoadBalance extends AbstractLoadBalance {

    private static final Random RANDOM = new Random();

    @Override
    public DivideUpstream doSelect(final List<DivideUpstream> upstreamList, final String ip) {
        // 计算所有后端节点的权重值
        int totalWeight = calculateTotalWeight(upstreamList);
        // 判断是否所有的节点为相同权重的情况
        boolean sameWeight = isAllUpStreamSameWeight(upstreamList);
        if (totalWeight > 0 && !sameWeight) {
            // 不是，则要加权随机
            // 加权随机的逻辑：
            return random(totalWeight, upstreamList);
        }
        // If the weights are the same or the weights are 0 then random
        // 如果各节点权重都相同，则按照总的节点数N，生成一个1-N之间的随机数X，然后选取出该X节点
        return random(upstreamList);
    }

    private boolean isAllUpStreamSameWeight(final List<DivideUpstream> upstreamList) {
        boolean sameWeight = true;
        int length = upstreamList.size();
        for (int i = 0; i < length; i++) {
            int weight = getWeight(upstreamList.get(i));
            if (i > 0 && weight != getWeight(upstreamList.get(i - 1))) {
                // Calculate whether the weight of ownership is the same
                sameWeight = false;
                break;
            }
        }
        return sameWeight;
    }

    private int calculateTotalWeight(final List<DivideUpstream> upstreamList) {
        // total weight
        int totalWeight = 0;
        for (DivideUpstream divideUpstream : upstreamList) {
            int weight = getWeight(divideUpstream);
            // Cumulative total weight
            totalWeight += weight;
        }
        return totalWeight;
    }

    private DivideUpstream random(final int totalWeight, final List<DivideUpstream> upstreamList) {
        // If the weights are not the same and the weights are greater than 0, then random by the total number of weights
        // 先拿到随机数为区间，为1-总权重值N之间
        int offset = RANDOM.nextInt(totalWeight);
        // Determine which segment the random value falls on
        // 遍历所有的节点，每次循环都用区间=区间-当前权重值，然后再看区间是否小于0；
        // 若小于0，则证明刚好位于当前节点的区间，返回当前节点；若没有，则继续循环
        for (DivideUpstream divideUpstream : upstreamList) {
            offset -= getWeight(divideUpstream);
            if (offset < 0) {
                return divideUpstream;
            }
        }
        // 最后返回第一个兜底
        return upstreamList.get(0);
    }

    private DivideUpstream random(final List<DivideUpstream> upstreamList) {
        return upstreamList.get(RANDOM.nextInt(upstreamList.size()));
    }
}
