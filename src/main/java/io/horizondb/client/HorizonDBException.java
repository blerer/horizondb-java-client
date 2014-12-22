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

import io.horizondb.model.ErrorCodes;
import io.horizondb.model.protocol.ErrorPayload;

/**
 *  Base class for the Exception thrown by the java client.
 */
public class HorizonDBException extends RuntimeException {

    /**
	 * 
	 */
    private static final long serialVersionUID = 3844334412343282479L;

    /**
     * The error code.
     */
    private final int code;

    /**
     * Creates an <code>HorizonDBException</code> from the specified <code>ErrorPayload</code>.
     * @param errorPayload the payload of the error message
     */
    public HorizonDBException(ErrorPayload errorPayload) {
        super(new StringBuilder().append("[ERROR: ")
                                 .append(errorPayload.getCode())
                                 .append("] ")
                                 .append(errorPayload.getMessage())
                                 .toString());

        this.code = errorPayload.getCode();
    }

    /**
     * Creates an <code>HorizonDBException</code> with the specified message.
     * @param message the error message
     */
    public HorizonDBException(String message) {
        super(message);
        this.code = ErrorCodes.INTERNAL_ERROR;
    }

    /**
     * Creates an <code>HorizonDBException</code> with the specified message and root cause.
     * @param message the error message
     * @param cause the error root cause
     */
    public HorizonDBException(String message, Throwable cause) {
        super(message, cause);
        this.code = ErrorCodes.INTERNAL_ERROR;
    }

    /**
     * Returns the error code.
     * @return the error code
     */
    public int getCode() {
        return this.code;
    }
}
