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
 *
 */

package org.apache.skywalking.apm.agent.core.context;

import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.conf.dynamic.ConfigurationDiscoveryService;
import org.apache.skywalking.apm.agent.core.conf.dynamic.watcher.IgnoreSuffixPatternsWatcher;
import org.apache.skywalking.apm.agent.core.conf.dynamic.watcher.SpanLimitWatcher;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.remote.GRPCChannelListener;
import org.apache.skywalking.apm.agent.core.remote.GRPCChannelManager;
import org.apache.skywalking.apm.agent.core.remote.GRPCChannelStatus;
import org.apache.skywalking.apm.agent.core.sampling.SamplingService;
import org.apache.skywalking.apm.util.StringUtil;

import java.util.Arrays;

@DefaultImplementor
public class ContextManagerExtendService implements BootService, GRPCChannelListener {
    private static final ILog LOGGER = LogManager.getLogger(ContextManagerExtendService.class);

    private volatile String[] ignoreSuffixArray = new String[0];

    private volatile GRPCChannelStatus status = GRPCChannelStatus.DISCONNECT;

    private IgnoreSuffixPatternsWatcher ignoreSuffixPatternsWatcher;

    private SpanLimitWatcher spanLimitWatcher;

    @Override
    public void prepare() {
        ServiceManager.INSTANCE.findService(GRPCChannelManager.class).addChannelListener(this);
    }

    @Override
    public void boot() {
        ignoreSuffixArray = Config.Agent.IGNORE_SUFFIX.split(",");
        ignoreSuffixPatternsWatcher = new IgnoreSuffixPatternsWatcher("agent.ignore_suffix", this);
        spanLimitWatcher = new SpanLimitWatcher("agent.span_limit_per_segment");
        if (LOGGER.isDebugEnable()) {
            LOGGER.debug("boot createTraceContext ignoreSuffixArray={}", ignoreSuffixArray);
        }

        ConfigurationDiscoveryService configurationDiscoveryService = ServiceManager.INSTANCE.findService(
                ConfigurationDiscoveryService.class);
        configurationDiscoveryService.registerAgentConfigChangeWatcher(spanLimitWatcher);
        configurationDiscoveryService.registerAgentConfigChangeWatcher(ignoreSuffixPatternsWatcher);

        handleIgnoreSuffixPatternsChanged();
    }

    @Override
    public void onComplete() {

    }

    @Override
    public void shutdown() {

    }

    public AbstractTracerContext createTraceContext(String operationName, boolean forceSampling) {
        AbstractTracerContext context;
        /*
         * Don't trace anything if the backend is not available.
         */
        if (!Config.Agent.KEEP_TRACING && GRPCChannelStatus.DISCONNECT.equals(status)) {
            return new IgnoredTracerContext();
        }
        if (LOGGER.isDebugEnable()) {
            LOGGER.debug("createTraceContext status={} operationName={}", status, operationName);
        }

        int suffixIdx = operationName.lastIndexOf(".");
        if (suffixIdx > -1 && Arrays.stream(ignoreSuffixArray)
                .anyMatch(a -> a.equals(operationName.substring(suffixIdx)))) {
            context = new IgnoredTracerContext();
            if (LOGGER.isDebugEnable()) {
                LOGGER.debug("createTraceContext match ignoreSuffixArray");
            }
        } else {
            SamplingService samplingService = ServiceManager.INSTANCE.findService(SamplingService.class);
            boolean trySampling = samplingService.trySampling(operationName);
            if (LOGGER.isDebugEnable()) {
                LOGGER.debug("createTraceContext forceSampling={} trySampling={}", forceSampling, trySampling);
            }
            if (forceSampling || trySampling) {
                context = new TracingContext(operationName, spanLimitWatcher);
            } else {
                context = new IgnoredTracerContext();
            }
        }
        if (LOGGER.isDebugEnable()) {
            try {
                LOGGER.debug("createTraceContext operationName={} segmentId={} traceId={}", operationName, context.getSegmentId(), context.getReadablePrimaryTraceId());
            } catch (Exception e) {
                LOGGER.error("createTraceContext error", e);
            }

        }
        return context;
    }

    @Override
    public void statusChanged(final GRPCChannelStatus status) {
        this.status = status;
    }

    public void handleIgnoreSuffixPatternsChanged() {
        if (StringUtil.isNotBlank(ignoreSuffixPatternsWatcher.getIgnoreSuffixPatterns())) {
            ignoreSuffixArray = ignoreSuffixPatternsWatcher.getIgnoreSuffixPatterns().split(",");
        }
    }
}
