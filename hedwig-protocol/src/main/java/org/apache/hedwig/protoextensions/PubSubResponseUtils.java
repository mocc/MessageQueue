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
package org.apache.hedwig.protoextensions;

import com.google.protobuf.ByteString;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.ProtocolVersion;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEventResponse;

public class PubSubResponseUtils {

    /**
     * Change here if bumping up the version number that the server sends back
     */
    public final static ProtocolVersion serverVersion = ProtocolVersion.VERSION_ONE;

    static PubSubResponse.Builder getBasicBuilder(StatusCode status) {
        return PubSubResponse.newBuilder().setProtocolVersion(serverVersion).setStatusCode(status);
    }

    public static PubSubResponse getSuccessResponse(long txnId) {
        return getBasicBuilder(StatusCode.SUCCESS).setTxnId(txnId).build();
    }

    public static PubSubResponse getSuccessResponse(long txnId, ResponseBody respBody) {
        return getBasicBuilder(StatusCode.SUCCESS).setTxnId(txnId)
               .setResponseBody(respBody).build();
    }

    public static PubSubResponse getResponseForException(PubSubException e, long txnId) {
        return getBasicBuilder(e.getCode()).setStatusMsg(e.getMessage()).setTxnId(txnId).build();
    }

    public static PubSubResponse getResponseForSubscriptionEvent(ByteString topic,
                                                                 ByteString subscriberId,
                                                                 SubscriptionEvent event) {
        SubscriptionEventResponse.Builder eventBuilder =
            SubscriptionEventResponse.newBuilder().setEvent(event);
        ResponseBody.Builder respBuilder =
            ResponseBody.newBuilder().setSubscriptionEvent(eventBuilder);
        PubSubResponse response = PubSubResponse.newBuilder()
                                  .setProtocolVersion(ProtocolVersion.VERSION_ONE)
                                  .setStatusCode(StatusCode.SUCCESS).setTxnId(0)
                                  .setTopic(topic).setSubscriberId(subscriberId)
                                  .setResponseBody(respBuilder).build();
        return response;
    }
}
