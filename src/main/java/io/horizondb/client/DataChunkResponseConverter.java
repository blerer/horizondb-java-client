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

import io.horizondb.model.protocol.DataHeaderPayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.Msgs;
import io.horizondb.model.schema.RecordSetDefinition;

/**
 * @author Benjamin
 *
 */
public class DataChunkResponseConverter implements ResponseConverter {

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordSet convert(Msg<?> response, MsgChannel channel) {
        
        DataHeaderPayload header = Msgs.getPayload(response);
        RecordSetDefinition definition = header.getDefinition();
        
        return new DefaultRecordSet(definition, new StreamedRecordIterator(definition, channel));
    }

}
