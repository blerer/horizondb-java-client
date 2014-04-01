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

import io.horizondb.model.core.records.TimeSeriesRecord;

/**
 * The definition of the data contained within the record set.
 * 
 * @author Benjamin
 */
public interface RecordSetDefinition {

    /**
     * Returns the index of specified field.
     * 
     * @param type the index of the record type
     * @param name the field name
     * @return the index of specified field.
     */
    int getFieldIndex(int type, String name);

    /**
     * Returns records instances corresponding to this <code>RecordSet</code> records.
     * 
     * @return records instances corresponding to this <code>RecordSet</code> records.
     */
    TimeSeriesRecord[] newRecords();
}
