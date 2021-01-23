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

package org.dromara.soul.plugin.alibaba.dubbo.proxy;

import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.rpc.service.GenericException;
import com.alibaba.dubbo.rpc.service.GenericService;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.dromara.soul.common.dto.MetaData;
import org.dromara.soul.common.exception.SoulException;
import org.dromara.soul.common.utils.ParamCheckUtils;
import org.dromara.soul.plugin.alibaba.dubbo.cache.ApplicationConfigCache;
import org.dromara.soul.plugin.api.dubbo.DubboParamResolveService;

/**
 * Alibaba dubbo proxy service is  use GenericService.
 *
 * @author xiaoyu(Myth)
 */
@Slf4j
public class AlibabaDubboProxyService {
    
    private final DubboParamResolveService dubboParamResolveService;
    
    /**
     * Instantiates a new Dubbo proxy service.
     *
     * @param dubboParamResolveService the generic param resolve service
     */
    public AlibabaDubboProxyService(final DubboParamResolveService dubboParamResolveService) {
        this.dubboParamResolveService = dubboParamResolveService;
    }
    
    /**
     * Generic invoker object.
     *
     * @param body     the body
     * @param metaData the meta data
     * @return the object
     * @throws SoulException the soul exception
     */
    public Object genericInvoker(final String body, final MetaData metaData) throws SoulException {
        // 找服务引用配置
        ReferenceConfig<GenericService> reference = ApplicationConfigCache.getInstance().get(metaData.getPath());
        // 为空则初始化一个服务引用配置
        if (Objects.isNull(reference) || StringUtils.isEmpty(reference.getInterface())) {
            ApplicationConfigCache.getInstance().invalidate(metaData.getPath());
            reference = ApplicationConfigCache.getInstance().initRef(metaData);
        }
        // 获取服务引用配置中的服务
        GenericService genericService = reference.get();
        try {
            // 处理dubbo服务的请求参数；为请求参数对；key为参数名数组，value为参数值数组
            Pair<String[], Object[]> pair;
            if (ParamCheckUtils.dubboBodyIsEmpty(body)) {
                pair = new ImmutablePair<>(new String[]{}, new Object[]{});
            } else {
                pair = dubboParamResolveService.buildParameter(body, metaData.getParameterTypes());
            }
            // 完成dubbo服务调用
            return genericService.$invoke(metaData.getMethodName(), pair.getLeft(), pair.getRight());
        } catch (GenericException e) {
            log.error("dubbo invoker have exception", e);
            throw new SoulException(e.getExceptionMessage());
        }
    }
}
