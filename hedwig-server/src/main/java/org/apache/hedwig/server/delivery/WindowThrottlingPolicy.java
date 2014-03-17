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
package org.apache.hedwig.server.delivery;

import static org.apache.hedwig.util.VarArgs.va;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowThrottlingPolicy implements ThrottlingPolicy {

    public static final int UNLIMITED = 0;

    private final Logger logger = LoggerFactory.getLogger(WindowThrottlingPolicy.class);

    private final String label;
    private final long messageWindowSize;
    long lastSeqIdConsumedUtil;

    public WindowThrottlingPolicy(String label, long lastSeqIdConsumedUtil, long messageWindowSize) {
        this.label = label;
        this.lastSeqIdConsumedUtil = lastSeqIdConsumedUtil;
        this.messageWindowSize = messageWindowSize;
    }

    @Override
    public boolean messageConsumed(long newSeqIdConsumed) {
        if (newSeqIdConsumed <= lastSeqIdConsumedUtil) {
            return false;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("{} moved consumed ptr from [} to {}.", va(label, lastSeqIdConsumedUtil, newSeqIdConsumed));
        }
        lastSeqIdConsumedUtil = newSeqIdConsumed;
        logger.info("server side: the consuming message ID is: "+newSeqIdConsumed);
        return true;
    }

    @Override
    public boolean shouldThrottle(long lastSeqIdDelivered) {
        if (messageWindowSize == UNLIMITED) {
            return false;
        }
        if (lastSeqIdDelivered - lastSeqIdConsumedUtil >= messageWindowSize) {
            return true;
        }
        return false;
    }

}
