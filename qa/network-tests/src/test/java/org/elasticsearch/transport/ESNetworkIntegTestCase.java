/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.transport;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.netty.NettyTransport;

import java.util.Collection;

@ESIntegTestCase.SuppressLocalMode
public abstract class ESNetworkIntegTestCase extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder();
        // randomize netty settings
        if (randomBoolean()) {
            builder.put(NettyTransport.WORKER_COUNT.getKey(), random().nextInt(3) + 1);
            builder.put(NettyTransport.CONNECTIONS_PER_NODE_RECOVERY.getKey(), random().nextInt(2) + 1);
            builder.put(NettyTransport.CONNECTIONS_PER_NODE_BULK.getKey(), random().nextInt(3) + 1);
            builder.put(NettyTransport.CONNECTIONS_PER_NODE_REG.getKey(), random().nextInt(6) + 1);
        }
        if (randomBoolean()) {
            builder.put(TransportSettings.PING_SCHEDULE.getKey(), randomIntBetween(100, 2000) + "ms");
        }
        return super.nodeSettings(nodeOrdinal);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(NettyPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return pluginList(NettyPlugin.class);
    }

}
