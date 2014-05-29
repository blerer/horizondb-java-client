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

import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.OpCode;

/**
 * @author Benjamin
 *
 */
public class ResponseConverterDispatcher implements ResponseConverter {

    private ResponseConverter defaultConverter = new DefaultResponseConverter();
    
    private ResponseConverter dataConverter = new DataChunkResponseConverter();
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RecordSet convert(Msg<?> response, MsgChannel channel) {
        
        if (response.getOpCode() == OpCode.DATA_HEADER) {
            
            return this.dataConverter.convert(response, channel);
        }
        
        return this.defaultConverter.convert(response, channel);
    }
}
