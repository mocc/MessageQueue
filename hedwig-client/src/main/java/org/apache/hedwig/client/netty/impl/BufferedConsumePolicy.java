/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hedwig.client.netty.impl;

import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BufferedConsumePolicy implements ConsumePolicy {

    private static final Logger logger = LoggerFactory.getLogger(BufferedConsumePolicy.class);

    // Counter for the number of consumed messages so far to buffer up before we
    // send the Consume message back to the server along with the last/largest
    // message seq ID seen so far in that batch.
    private int numConsumedMessagesInBuffer = 0;
    private MessageSeqId lastMessageSeqId = null;
    private final int consumedMessagesBufferSize;

    public BufferedConsumePolicy(int consumedMessagesBufferSize) {
        this.consumedMessagesBufferSize = consumedMessagesBufferSize;
    }

    private synchronized boolean updateLastMessageSeqId(MessageSeqId seqId) {
        if (null != lastMessageSeqId && seqId.getLocalComponent() <= lastMessageSeqId.getLocalComponent()) {
            return false;
        }
        ++numConsumedMessagesInBuffer;
        lastMessageSeqId = seqId;
        if (numConsumedMessagesInBuffer >= consumedMessagesBufferSize) {
            numConsumedMessagesInBuffer = 0;
            lastMessageSeqId = null;
            return true;
        }
        return false;
    }

    @Override
    public void messageConsumed(Message message, Consumer consumer) {
        // Update these variables only if we are auto-sending consume
        // messages to the server. Otherwise the onus is on the client app
        // to call the Subscriber consume API to let the server know which
        // messages it has successfully consumed.
        if (updateLastMessageSeqId(message.getMsgId())) {
            // Send the consume request and reset the consumed messages buffer
            // variables. We will use the same Channel created from the
            // subscribe request for the TopicSubscriber.
            if (logger.isDebugEnabled()) {
                logger.debug("Consume message {} when reaching consumed message buffer limit.", message.getMsgId());
            }
            consumer.consume(message.getMsgId());
        }
    }

}
