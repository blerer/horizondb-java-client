/**
 * Copyright 2014 Benjamin Lerer
 * 
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

import io.horizondb.model.core.Record;
import io.horizondb.model.core.iterators.DefaultRecordIterator;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.util.ArrayList;
import java.util.Map.Entry;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Range;

/**
 * <code>RecordSet</code> backed up by a list of records.
 * 
 * @author Benjamin
 *
 */
final class PartitionAwareRecordSet extends DefaultRecordSet {

    /**
     * The underlying records per time range.
     */
    private final LinkedListMultimap<Range<Long>, ? extends Record> recordsPerPartition;
    
    /**
     * The current position within the list.
     */
    private int index = -1;
    
    /**
     * Creates a new <code>PartitionAwareRecordSet</code>. 
     * 
     * @param definition the time series definition
     * @param records the records of this <code>DefaultRecordSet</code>.
     */
    public PartitionAwareRecordSet(TimeSeriesDefinition definition, LinkedListMultimap<Range<Long>, ? extends Record> recordsPerPartition) {
        
        super(definition, new DefaultRecordIterator(new ArrayList<Record>(recordsPerPartition.values())));

        this.recordsPerPartition = recordsPerPartition;
    }
    
    /**
     * Returns the remaining content of this <code>DefaultRecordSet</code> as a <code>List</code>.
     * @return the remaining content of this <code>DefaultRecordSet</code> as a <code>List</code>.
     */
    public ListMultimap<Range<Long>, ? extends Record> asMultimap() {
        
        if (this.index == -1) {
            return Multimaps.unmodifiableListMultimap(this.recordsPerPartition);
        }
        
        
        return Multimaps.unmodifiableListMultimap(truncateMultimap(this.recordsPerPartition, this.index));
    }

    /**
     * Truncates the entries of the specified <code>LinkedListMultimap</code> which are before the specified entry.
     * 
     * @param multimap the multimap to truncate
     * @param entryIndex the index of the entry where the truncation should occurs
     * @return the truncated <code>LinkedListMultimap</code>.
     */
    private static <K, V> ListMultimap<K, V> truncateMultimap(LinkedListMultimap<K, V> multimap, int entryIndex) {
        
        ListMultimap<K, V> truncated = LinkedListMultimap.create();
        
        int i = 0;
        
        for (Entry<K, V> entry : multimap.entries()) {
            
            if (entryIndex >= i++) {
                
                continue;
            }
            
            truncated.put(entry.getKey(), entry.getValue());
        }
        
        return truncated;
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    protected void onNext() {

        this.index++;
    }
}
