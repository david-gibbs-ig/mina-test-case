
/*
 *  Based on Mina Chat Example DG
 *  
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public class Consumer {

	private static Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
	private final SocketConnector connector = new NioSocketConnector();
	private int expectedMessages = Producer.DEFAULT_MSG_COUNT;
	
	public int getExpectedMessages() {
		return expectedMessages;
	}

	public void setExpectedMessages(int expectedMessages) {
		this.expectedMessages = expectedMessages;
	}

	public Consumer() {
		ProtocolCodecFilter fixCodecFilter = new ProtocolCodecFilter(new TextLineCodecFactory());
		ConsumerProtocolHandler consumer = new Consumer.ConsumerProtocolHandler(connector, this.expectedMessages);
		connector.getFilterChain().addLast("codec", fixCodecFilter);
		connector.setHandler(consumer);
	}

	public void write(String news) {
		for (IoSession session : this.connector.getManagedSessions().values()) {
			LOGGER.info("writing news.");
			session.write(news);
		}
	}

	public boolean connect(InetSocketAddress inetSocketAddress) throws UnknownHostException {
		ConnectFuture future1 = connector.connect(inetSocketAddress);
		future1.awaitUninterruptibly();
		LOGGER.info("done waiting, connected = {}", future1.isConnected());
		if (future1.isConnected()) {
			return true;
		} else {
			return false;
		}
	}

	static class ConsumerProtocolHandler extends IoHandlerAdapter {
		private final SocketConnector connector;
		private final int expectedMessages;
		private int receivedMessages = 0;
		
		public ConsumerProtocolHandler (SocketConnector socketConnector, int expectedMessages) {
			this.connector = socketConnector;
			this.expectedMessages = expectedMessages;
		}
		
		@Override
		public void exceptionCaught(IoSession session, Throwable cause) {
			LOGGER.error("Unexpected exception.",cause);
			cause.printStackTrace();
			// Close connection when unexpected exception is caught.
			session.closeNow();
		}

		@Override
		public void messageReceived(IoSession session, Object message) {
			++receivedMessages;
			LOGGER.debug("received : {}, Msg Number {}", message, receivedMessages );
			if (receivedMessages == expectedMessages) {
				session.closeNow();
				this.connector.dispose();
			}
		}

		@Override
		public void sessionClosed(IoSession session) throws Exception {
			LOGGER.info("Session closed.");
			this.connector.dispose();
		}
	}

	public static void main(String[] args) throws Exception {
		Consumer consumer = new Consumer();
		consumer.connect(new InetSocketAddress(InetAddress.getLocalHost(), Producer.DEFAULT_PORT));
		consumer.write("Hello");
	}

}
