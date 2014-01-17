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

import io.horizondb.model.PartitionId;
import io.horizondb.model.Query;
import io.horizondb.model.RecordBatch;
import io.horizondb.model.TimeRange;
import io.horizondb.model.core.Record;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.OpCode;
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
	    return this.seriesDefinition.getSeriesName();
    }
	
	public RecordSet.Builder newRecordSetBuilder() {
		
		return new RecordSetBuilder(this.seriesDefinition);
	}
	
	public void write(RecordSet recordSet) {

		Validate.isTrue(this.seriesDefinition.equals(recordSet.getTimeSeriesDefinition()), 
		                "the recordSet is not associated to this time series but to " 
		                		+ this.databaseDefinition.getName() + "." + this.seriesDefinition.getSeriesName());
		
		PartitionAwareRecordSet partitionAwareRecordSet = (PartitionAwareRecordSet) recordSet; 
		
		ListMultimap<TimeRange, ? extends Record> multimap = partitionAwareRecordSet.asMultimap();
		
		for (TimeRange range : multimap.keySet()) {
		
            List<? extends Record> records = multimap.get(range);

            RecordBatch batch = new RecordBatch(this.databaseDefinition.getName(),
                                                this.seriesDefinition.getSeriesName(),
                                                range.getStart(),
                                                records);

            this.manager.send(Msg.newRequestMsg(OpCode.BATCH_INSERT, batch));
		}
	}
	
	public RecordSet read(long startTimeInMillis, long endTimeInMillis) {

		TimeRange range = new TimeRange(startTimeInMillis, endTimeInMillis);

		TimeRange partitionTimeRange = this.seriesDefinition.getPartitionTimeRange(startTimeInMillis);

		PartitionId id = new PartitionId(this.seriesDefinition.getDatabaseName(),
		                                 this.seriesDefinition.getSeriesName(),
		                                 partitionTimeRange.getStart());

		Msg<Query> query = Msg.newRequestMsg(OpCode.QUERY, new Query(id, range));

		return new DefaultRecordSet(this.seriesDefinition, new StreamedRecordIterator(this.seriesDefinition,
		                                                                  this.manager.getConnection(),
		                                                                  query));
	}
}
