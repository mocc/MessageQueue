/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.client.netty.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.handlers.AbstractResponseHandler;
import org.apache.hedwig.client.handlers.PublishResponseHandler;
import org.apache.hedwig.client.handlers.QueueResponseHandler;
import org.apache.hedwig.client.handlers.UnsubscribeResponseHandler;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;

public class NonSubscriptionChannelPipelineFactory extends ClientChannelPipelineFactory {

    public NonSubscriptionChannelPipelineFactory(ClientConfiguration cfg,
                                                 AbstractHChannelManager channelManager) {
        super(cfg, channelManager);
    }

    @Override
    protected Map<OperationType, AbstractResponseHandler> createResponseHandlers() {
        Map<OperationType, AbstractResponseHandler> handlers =
            new HashMap<OperationType, AbstractResponseHandler>();
        handlers.put(OperationType.PUBLISH,
                     new PublishResponseHandler(cfg, channelManager));
        handlers.put(OperationType.UNSUBSCRIBE,
                     new UnsubscribeResponseHandler(cfg, channelManager));
        //add for message queue semantics
        handlers.put(OperationType.QUEUE_MGNT, 
        		new QueueResponseHandler(cfg, channelManager));
        return handlers;
    }

}
