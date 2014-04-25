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

import io.horizondb.model.protocol.HqlQueryPayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.Msgs;
import io.horizondb.model.protocol.SetDatabasePayload;
import io.horizondb.model.schema.DatabaseDefinition;

import java.io.Closeable;
import java.net.InetSocketAddress;

import static org.apache.commons.lang.Validate.notEmpty;

/**
 * @author Benjamin
 *
 */
public class HorizonClient implements Closeable {

	private final ConnectionManager manager;
	
	/**
	 * The client configuration.
	 */
	private final ClientConfiguration configuration;
	
	/**
	 * 
	 */
	public HorizonClient(int port) {
		
		this(new InetSocketAddress("localhost", port));
	}
	
	/**
	 * 
	 */
	public HorizonClient(InetSocketAddress serverAddress) {

		this.configuration = new ClientConfiguration(serverAddress);
		this.manager = new ConnectionManager(this.configuration);
	}

	/**
	 * Creates a new database with the specified name.
	 * 
	 * @param name the new database name.
	 * @return the new database.
	 */
	public Database newDatabase(String name) {
		
		notEmpty(name, "the name parameter must not be empty.");
		
		DatabaseDefinition definition = new DatabaseDefinition(name);

		this.manager.send(Msgs.newHqlQueryMsg("", "CREATE DATABASE " + name + " ;"));
		
		return new Database(this.manager, definition);
	}
	
	/**
	 * Returns the database with the specified name.
	 * 
	 * @param name the database name.
	 * @return the database with the specified name.
	 */
	public Database getDatabase(String name) {
		
		notEmpty(name, "the name parameter must not be empty.");
		
		Msg<HqlQueryPayload> request = Msgs.newHqlQueryMsg("", "USE " + name + " ;");

		SetDatabasePayload payload = Msgs.getPayload(this.manager.send(request));
		
		return new Database(this.manager, payload.getDefinition());
	}
	
	/**
	 * Sets the query timeout in seconds.
	 * 
	 * @param queryTimeout the new query timeout in seconds.
	 */
	public void setQueryTimeoutInSeconds(int queryTimeout) {
		
		this.configuration.setQueryTimeoutInSeconds(queryTimeout);		
	}
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() {

    	this.manager.close();
    }
  }

