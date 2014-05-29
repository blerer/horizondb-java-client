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

import io.horizondb.model.protocol.ErrorPayload;
import io.horizondb.model.protocol.Msg;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Handle the sending and retrieval of messages.
 * 
 * @author Benjamin
 *
 */
 class DefaultMsgChannel implements MsgChannel {
	 
	 /**
	  * The channel.
	  */
	 private final Channel channel;
	 
	 /**
	  * The queue used to store the response messages.
	  */
	 private final BlockingQueue<Msg<?>> queue;
	 
	 /**
	  * The query timeout in second.
	  */
	 private final int queryTimeoutInSecond;
	 
	/**
	 * @param channel 
	 * 
	 */
	public DefaultMsgChannel(Channel channel, int queryTimeoutInSecond) {
		
		this.channel = channel;
		this.queryTimeoutInSecond = queryTimeoutInSecond;
		this.queue = ((ClientHandler) this.channel.pipeline().last()).getQueue();
	}

	/**
     * {@inheritDoc}
     */
    @Override
    public void sendRequest(Msg<?> request) {
	    
    	this.queue.clear();
		
		ChannelFuture future = this.channel.writeAndFlush(request);
		future.awaitUninterruptibly();
		
		if (!future.isSuccess()) {
			
			throw new HorizonDBException("",  future.cause());
		}
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Msg<?> awaitResponse() {
        
        return awaitResponse(this.queryTimeoutInSecond);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Msg<?> awaitResponse(int timeoutInSeconds) {
	    try {
	        
			Msg<?> response = this.queue.poll(timeoutInSeconds, TimeUnit.SECONDS);
			
			if (response == null) {
				
				throw new QueryTimeoutException("No response has been received for more than " 
						+ timeoutInSeconds + " seconds.");
			}
			
			if (!response.getHeader().isSuccess()) {
				
				throw new HorizonDBException((ErrorPayload) response.getPayload());
			}
			
			return response;
	        
        } catch (InterruptedException e) {
	                	
        	Thread.currentThread().interrupt();
        	throw new HorizonDBException("", e);
        }
    }
	
	/**
     * {@inheritDoc}
     */
    @Override
    public void close() {
    	
    	this.channel.disconnect();
    }
}
