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

import java.util.NoSuchElementException;

import io.horizondb.model.core.records.TimeSeriesRecord;

/**
 * @author Benjamin
 *
 */
final class EmptyRecordSetDefinition implements RecordSetDefinition {

    /**
     * An empty time series array.
     */
    private static final TimeSeriesRecord[] EMPTY_RECORDS = new TimeSeriesRecord[0];
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int getFieldIndex(int type, String name) {
        throw new NoSuchElementException("No record has been definied with index " + type);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeSeriesRecord[] newRecords() {
        return EMPTY_RECORDS;
    }
}
