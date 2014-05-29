/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.horizondb.client;

import java.io.Closeable;

import io.horizondb.model.protocol.Msg;

/**
 * @author Benjamin
 *
 */
interface MsgChannel extends Closeable {

    /**
     * Send the specified request to the server.
     * @param request the request sent to the server.
     */
    void sendRequest(Msg<?> request);

    /**
     * Await for a response from the server.
     * @return the message received from the server
     */
    Msg<?> awaitResponse();

    /**
     * Await for the specified a amount of time for a response from the server.
     * @return the message received from the server
     */
    Msg<?> awaitResponse(int timeoutInSeconds);
}