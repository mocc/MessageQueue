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
package org.apache.hedwig.server.persistence;

public class ScanCallbackWithContext {
    ScanCallback scanCallback;
    Object ctx;

    public ScanCallbackWithContext(ScanCallback callback, Object ctx) {
        this.scanCallback = callback;
        this.ctx = ctx;
    }

    public ScanCallback getScanCallback() {
        return scanCallback;
    }

    public Object getCtx() {
        return ctx;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ScanCallbackWithContext)) {
            return false;
        }
        ScanCallbackWithContext otherCb =
            (ScanCallbackWithContext) other;
        // Ensure that it was same callback & same ctx
        return scanCallback == otherCb.scanCallback &&
               ctx == otherCb.ctx;
    }

    @Override
    public int hashCode() {
        return scanCallback.hashCode();
    }

}
