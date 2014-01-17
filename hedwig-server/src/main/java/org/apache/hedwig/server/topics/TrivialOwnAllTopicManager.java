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
package org.apache.hedwig.server.topics;

import java.net.UnknownHostException;
import java.util.concurrent.ScheduledExecutorService;

import com.google.protobuf.ByteString;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.HedwigSocketAddress;

public class TrivialOwnAllTopicManager extends AbstractTopicManager {

    public TrivialOwnAllTopicManager(ServerConfiguration cfg, ScheduledExecutorService scheduler)
            throws UnknownHostException {
        super(cfg, scheduler);
    }

    @Override
    protected void realGetOwner(ByteString topic, boolean shouldClaim,
                                Callback<HedwigSocketAddress> cb, Object ctx) {

        TopicStats stats = topics.getIfPresent(topic);
        if (null != stats) {
            cb.operationFinished(ctx, addr);
            return;
        }

        notifyListenersAndAddToOwnedTopics(topic, cb, ctx);
    }

    @Override
    protected void postReleaseCleanup(ByteString topic, Callback<Void> cb, Object ctx) {
        // No cleanup to do
        cb.operationFinished(ctx, null);
    }

    @Override
    public void stop() {
        // do nothing
    }
}
