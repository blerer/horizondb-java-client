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
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.SetDatabasePayload;
import io.horizondb.model.schema.DatabaseDefinition;
import io.netty.channel.Channel;

import java.io.IOException;

/**
 * @author Benjamin
 *
 */
class DefaultConnection implements Connection {

	 /**
	  * The client configuration.
	  */
	 private final ClientConfiguration configuration;
	 
	 /**
	  * The channel.
	  */
	 private final MsgChannel channel;
	 
	 /**
	  * The response converter.
	  */
	 private final ResponseConverter converter;
	 
	 /**
	  * The database definition.
	  */
	 private DatabaseDefinition databaseDefinition;
	 
	/**
	 * @param channel 
	 * 
	 */
	public DefaultConnection(ClientConfiguration configuration, Channel channel, ResponseConverter converter) {
		
		this.configuration = configuration;
		this.channel = new DefaultMsgChannel(channel, configuration.getQueryTimeoutInSeconds());
		this.converter = converter;
	}
	
    /**
     * {@inheritDoc}
     */
    @Override
    public String getDatabase() {
        
        if (this.databaseDefinition == null) {
            return null;
        }
        
        return this.databaseDefinition.getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordSet execute(String query) {

        Msg<HqlQueryPayload> request = Msgs.newHqlQueryMsg(getDatabase(), query);
            
        this.channel.sendRequest(request);
        
        Msg<?> response = this.channel.awaitResponse(this.configuration.getQueryTimeoutInSeconds());
        
        if (response.getOpCode() == OpCode.SET_DATABASE) {
            
            SetDatabasePayload payload = Msgs.getPayload(response);
            this.databaseDefinition = payload.getDefinition();
        }
        
        return this.converter.convert(response, this.channel);
    }

    /**
	 * {@inheritDoc}
	 */
    @Override
    public void close() throws IOException {
    	
    	this.channel.close();
    }
}
