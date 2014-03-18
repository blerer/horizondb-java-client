/**
 * Copyright 2013 Benjamin Lerer
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

import io.horizondb.model.TimeRange;
import io.horizondb.model.core.Record;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.Msgs;
import io.horizondb.model.protocol.QueryPayload;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.util.List;

import org.apache.commons.lang.Validate;

import com.google.common.collect.ListMultimap;

/**
 * Represents a logical time series on a server.
 * 
 * @author Benjamin
 *
 */
public final class TimeSeries {
    
    /**
     * The manager managing the connections to the servers. 
     */
	private final ConnectionManager manager;
	
	/**
	 * The database definition.
	 */
    private final DatabaseDefinition databaseDefinition;
	
	/**
	 * The definition of this time series.
	 */
	private final TimeSeriesDefinition seriesDefinition;
	
	/**
	 * 
	 */
	TimeSeries(ConnectionManager manager, 
	           DatabaseDefinition databaseDefinition, 
	           TimeSeriesDefinition seriesDefinition) {
		
		this.manager = manager;
		this.databaseDefinition = databaseDefinition;
		this.seriesDefinition = seriesDefinition;
	}

	/**
	 * Returns the time series name.
	 * 
	 * @return the time series name.
	 */
    public String getName() {
	    return this.seriesDefinition.getName();
    }
	
    /**
     * Creates a new <code>RecordSet.Builder</code> instance.
     * @return a new <code>RecordSet.Builder</code> instance.
     */
	public RecordSet.Builder newRecordSetBuilder() {
		
		return new RecordSetBuilder(this.seriesDefinition);
	}
	
	/**
	 * Writes the specified set of records into this time series.
	 * 
	 * @param recordSet the set of records to write.
	 */
	public void write(RecordSet recordSet) {

	    Validate.isTrue(recordSet instanceof PartitionAwareRecordSet, 
	                    "RecordSet of type " + recordSet.getClass() + " are not supported.");
	    
		Validate.isTrue(isAssociatedToThisTimeSeries(recordSet), 
		                "the recordSet is not associated to " 
		                		+ this.databaseDefinition.getName() + "." + this.seriesDefinition.getName());

		PartitionAwareRecordSet partitionAwareRecordSet = (PartitionAwareRecordSet) recordSet; 
		
		ListMultimap<TimeRange, ? extends Record> multimap = partitionAwareRecordSet.asMultimap();
		
		for (TimeRange range : multimap.keySet()) {
		
            List<? extends Record> records = multimap.get(range);

            this.manager.send(Msgs.newBulkWriteRequest(this.databaseDefinition.getName(),
                                                       this.seriesDefinition.getName(),
                                                       range,
                                                       records));
		}
	}
	
	/**
	 * Reads all the records of this time series that are included into the specified time range. 
	 * 
	 * @param startTimeInMillis the start time in millisecond since epoch
	 * @param endTimeInMillis the end time in millisecond since epoch
	 * @return a <code>RecordSet</code> containing all the records of this time series that are included into 
	 * the specified time range
	 */
	public RecordSet read(long startTimeInMillis, long endTimeInMillis) {

		TimeRange range = new TimeRange(startTimeInMillis, endTimeInMillis);

		Msg<QueryPayload> query = Msgs.newQueryRequest(this.databaseDefinition.getName(),
		                                               this.seriesDefinition.getName(),
		                                               range);

		return new DefaultRecordSet(this.seriesDefinition, new StreamedRecordIterator(this.seriesDefinition,
		                                                                  this.manager.getConnection(),
		                                                                  query));
	}
	
    /**
     * Returns <code>true</code> if the specified record set is associated to this time series.
     * 
     * @param recordSet the record set to check
     * @return <code>true</code> if the specified record set is associated to this time series <code>false</code>
     * otherwise
     */
    private boolean isAssociatedToThisTimeSeries(RecordSet recordSet) {
        
        final RecordSetDefinition recordSetDefinition = recordSet.getRecordSetDefinition();
        
        return (recordSetDefinition instanceof TimeSeriesDefinitionAdapter) &&
                ((TimeSeriesDefinitionAdapter) recordSetDefinition).getTimeSeriesDefinition().equals(this.seriesDefinition);
    }
}
