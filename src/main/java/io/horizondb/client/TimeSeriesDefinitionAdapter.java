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

import static org.apache.commons.lang.Validate.notNull;

import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * Adapts <code>TimeSeriesDefinition</code> to the <code>RecordSetDefinition</code> interface.
 * 
 * @author Benjamin
 *
 */
final class TimeSeriesDefinitionAdapter implements RecordSetDefinition {

    /**
     * The adapted class.
     */
    private final TimeSeriesDefinition definition;
        
    /**
     * Adapts the specified <code>TimeSeriesDefinition</code> to the 
     * <code>RecordSetDefinition</code> interface.
     * 
     * @param definition the time series definition to adapt.
     */
    public TimeSeriesDefinitionAdapter(TimeSeriesDefinition definition) {
        
        notNull(definition, "the definition parameter must not be null.");
        this.definition = definition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFieldIndex(int type, String name) {
        return this.definition.getFieldIndex(type, name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesRecord[] newRecords() {
        return this.definition.newRecords();
    }
    
    /**
     * Returns the adapted time series definition.
     * @return the adapted time series definition.
     */
    TimeSeriesDefinition getTimeSeriesDefinition() {
        
        return this.definition;
    }
}
