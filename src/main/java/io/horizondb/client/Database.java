/**
 * Copyright 2013-2014 Benjamin Lerer
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

import io.horizondb.model.protocol.GetTimeSeriesResponsePayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.Msgs;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import static org.apache.commons.lang.Validate.notEmpty;
import static org.apache.commons.lang.Validate.notNull;

/**
 * Represents a logical database on a server.
 * 
 * @author Benjamin
 *
 */
public final class Database {

    /**
     * The manager managing the connections to the server.
     */
	private final ConnectionManager manager;
	
	/**
	 * The database definition.
	 */
	private final DatabaseDefinition definition;
	   
    /**
     * Returns the database name.
     * @return the database name.
     */
    public String getName() {
        return this.definition.getName();
    }
	
    /**
     * Creates a <code>TimeSeriesDefinition.Builder</code> to build a time series with the specified name.
     * 
     * @param seriesName the time series name
     * @return a <code>TimeSeriesDefinition.Builder</code> to build a time series with the specified name.
     */
	public TimeSeriesDefinition.Builder newTimeSeriesDefinitionBuilder(String seriesName) {
		
		return this.definition.newTimeSeriesDefinitionBuilder(seriesName);
	}
	
	/**
	 * Creates the specified time series.
	 * 
	 * @param timeSeriesDefinition the time series definition
	 * @return the time series corresponding to the specified definition.
	 * @throws IllegalArgumentException if the time series definition is <code>null</code>
	 * @throws HorizonDBException if the time series cannot be created
	 */
	public TimeSeries createTimeSeries(TimeSeriesDefinition timeSeriesDefinition) {

	    notNull(timeSeriesDefinition, "the timeSeriesDefinition parameter must not be null.");
	    
		this.manager.send(Msgs.newHqlQueryMsg(this.definition.getName(), timeSeriesDefinition.toHql()));
		
		return new TimeSeries(this.manager, this.definition, timeSeriesDefinition);
	}

	/**
	 * Returns the time series with the specified name.
	 * 
	 * @param seriesName the time series name
	 * @return the time series with the specified name.
	 */
    public TimeSeries getTimeSeries(String seriesName) {
    	
        notEmpty(seriesName, "the seriesName parameter must not be empty.");

    	Msg<?> response = this.manager.send(Msgs.newGetTimeSeriesRequest(getName(), seriesName));
	    
    	GetTimeSeriesResponsePayload payload = Msgs.getPayload(response);
    	
    	return new TimeSeries(this.manager, this.definition, payload.getDefinition());
    }
    
    /**
     * Creates a new <code>Database</code> instance with the specified definition.
     * 
     * @param manager the connection manager
     * @param definition the database definition
     */
    Database(ConnectionManager manager, DatabaseDefinition definition) {

        this.manager = manager;
        this.definition = definition;
    }
}
