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

import java.io.Closeable;
import java.net.InetSocketAddress;

/**
 * Entry point to connect to an <code>HorizonDB</code> database server.
 * 
 * @author Benjamin
 *
 */
public final class HorizonDB implements Closeable {
	
	/**
	 * The session manager.
	 */
	private ConnectionManager connectionManager;
	
	/**
     * Creates a connection to the database server.
     * 
     * @return a connection to the database server
     */
    public Connection newConnection() {
        
        return this.connectionManager.getSession();
    }
	
	/**
	 * Creates a connection to the specified database.
	 * 
	 * @param database the name of the database to which the connection will connect to
	 * @return a connection to the specified database
	 */
	public Connection newConnection(String database) {
	    
	    Connection connection = newConnection();
	    connection.execute("USE " + database + ";");
	    
	    return connection;
	}
	
	/**
     * Creates a new <code>Builder</code> to build an <code>HorizonDB</code> client for a local server. 
     *    
     * @param serverAddress the server address
     * @return a <code>Builder</code> to build an <code>HorizonDB</code> client for a local server. 
     */
	public static Builder newBuilder(int port) {
	    
	    return newBuilder(new InetSocketAddress("localhost", port));
	}
	
	/**
	 *  Creates a new <code>Builder</code> to build an <code>HorizonDB</code> client for a remote server. 
	 *    
	 * @param serverAddress the server address
	 * @return  a <code>Builder</code> to build an <code>HorizonDB</code> client for a remote server. 
	 */
    public static Builder newBuilder(InetSocketAddress serverAddress) {
        
        return new Builder(serverAddress);
    }
	    
    /**
     * 
     * {@inheritDoc}
     */
	@Override
    public void close() {
	    
	    this.connectionManager.close();
    }
		   
    /**
     * 
     */
    private HorizonDB(ClientConfiguration configuration) {

        this.connectionManager = new ConnectionManager(configuration);
    }

    /**
	 * The HorizonDB builder.
	 */
	public static final class Builder {
	    
	    /**
	     * The client configuration.
	     */
	    private final ClientConfiguration configuration;
	    
	    /**
	     * Sets the query timeout in seconds.
	     * 
	     * @param queryTimeout the new query timeout in seconds.
	     * @return this builder
	     */
	    public Builder setQueryTimeoutInSeconds(int queryTimeout) {
	        
	        this.configuration.setQueryTimeoutInSeconds(queryTimeout);  
	        return this;
	    }
	    
	    /**
	     * Creates a new <code>Builder</code> that use the specified server.	    
	     * @param serverAddress the server address
	     */
	    private Builder(InetSocketAddress serverAddress) {
	        
	        this.configuration = new ClientConfiguration(serverAddress);
	    }
	    
	    /**
	     * Builds a new <code>HorizonDB</code> instance.
	     * 
	     * @return a new <code>HorizonDB</code> instance.
	     */
	    public HorizonDB build() {
	        return new HorizonDB(this.configuration);
	    }
	}
  }

