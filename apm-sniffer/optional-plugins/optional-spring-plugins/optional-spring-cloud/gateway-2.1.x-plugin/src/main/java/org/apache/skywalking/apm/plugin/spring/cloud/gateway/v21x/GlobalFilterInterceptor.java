/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.skywalking.apm.plugin.spring.cloud.gateway.v21x;

import org.apache.skywalking.apm.agent.core.context.CarrierItem;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.ContextSnapshot;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.apache.skywalking.apm.util.StringUtil;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.ServerWebExchangeDecorator;
import org.springframework.web.server.adapter.DefaultServerWebExchange;

import java.lang.reflect.Method;

public class GlobalFilterInterceptor implements InstanceMethodsAroundInterceptor {
    private static final ILog LOGGER = LogManager.getLogger(GlobalFilterInterceptor.class);
    private String operationName = "SpringCloudGateway/GlobalFilter";

    public static EnhancedInstance getInstance(Object o) {
        EnhancedInstance instance = null;
        if (o instanceof DefaultServerWebExchange) {
            instance = (EnhancedInstance) o;
        } else if (o instanceof ServerWebExchangeDecorator) {
            ServerWebExchange delegate = ((ServerWebExchangeDecorator) o).getDelegate();
            return getInstance(delegate);
        }
        return instance;
    }

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
                             MethodInterceptResult result) throws Throwable {
        ServerWebExchange exchange = (ServerWebExchange) allArguments[0];
        EnhancedInstance enhancedInstance = getInstance(exchange);
        if (enhancedInstance == null) {
            LOGGER.warn("GlobalFilterInterceptor instance={}", enhancedInstance);
            return;
        }
        String simpleName = method.getDeclaringClass().getSimpleName();
        if (StringUtil.isNotBlank(simpleName)){
            operationName = "SpringCloudGateway/" + simpleName;
        }
        AbstractSpan span = ContextManager.createLocalSpan(operationName);
//        AbstractSpan span = ContextManager.createEntrySpan(operationName, carrier);
        if (enhancedInstance.getSkyWalkingDynamicField() != null) {
            ContextSnapshot parentSpan = (ContextSnapshot) enhancedInstance.getSkyWalkingDynamicField();
            ContextManager.continuedAsync(parentSpan);
            LOGGER.debug("GlobalFilterInterceptor continued parentSpan operationName={} endpoint={} traceId={} segmentId={} spanId={}",
                    operationName, parentSpan.getParentEndpoint(), parentSpan.getTraceId(), parentSpan.getTraceSegmentId(), parentSpan.getSpanId());
        }
        span.setComponent(ComponentsDefine.SPRING_CLOUD_GATEWAY);
        SpanLayer.asHttp(span);
        Tags.URL.set(span, exchange.getRequest().getURI().toString());
        Tags.HTTP.METHOD.set(span, exchange.getRequest().getMethodValue());
        ContextSnapshot capture = ContextManager.capture();
        LOGGER.debug("GlobalFilterInterceptor operationName={} traceId={} segmentId={} spanId={}",
                operationName, capture.getTraceId(), capture.getTraceSegmentId(), capture.getSpanId());
        enhancedInstance.setSkyWalkingDynamicField(capture);
        span.prepareForAsync();
        ContextManager.stopSpan(span);
        exchange.getAttributes().put(operationName, span);
    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
                              Object ret) throws Throwable {

        ServerWebExchange exchange = (ServerWebExchange) allArguments[0];
        if (exchange.getAttributes().containsKey(operationName)) {
            AbstractSpan span = (AbstractSpan) exchange.getAttributes().get(operationName);
            LOGGER.debug("GlobalFilterInterceptor afterMethod {}", operationName);
            if (span != null) {
                try {
                    span.asyncFinish();
                } catch (Exception ignored) {
                }
                exchange.getAttributes().remove(operationName);
            }
        }
        return ret;
    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
                                      Class<?>[] argumentsTypes, Throwable t) {
        ContextManager.activeSpan().log(t);
    }
}
