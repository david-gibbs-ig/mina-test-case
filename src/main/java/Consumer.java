
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
import java.util.concurrent.atomic.LongAdder;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;

/**
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public class Consumer {

	private static Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
	private final SocketConnector connector = new NioSocketConnector();
	private final LongAdder totalMessages = new LongAdder();
	
	public Consumer() {
		ProtocolCodecFilter fixCodecFilter = new ProtocolCodecFilter(new TextLineCodecFactory());
		ConsumerProtocolHandler consumer = 
			new Consumer.ConsumerProtocolHandler(connector, totalMessages);
		connector.getFilterChain().addLast("codec", fixCodecFilter);
		connector.setHandler(consumer);
	}

	public void write(String message) {
		for (IoSession session : this.connector.getManagedSessions().values()) {
			LOGGER.info("writing {}", message);
			session.write(message);
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
		private int receivedMessages = 0;
		private final LongAdder totalMessages;
		
		public ConsumerProtocolHandler (SocketConnector socketConnector, LongAdder totalMessages) {
			this.connector = socketConnector;
			this.totalMessages = totalMessages;
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
			this.totalMessages.increment();
			LOGGER.debug("received : {}, Msg Number {}", message, receivedMessages );
			if (message.toString().contains(Producer.GOODBYE)) {
				LOGGER.info("{} received, closing session", Producer.GOODBYE);
				LOGGER.info("{} messages received", this.totalMessages);
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
		LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
		JoranConfigurator configurator = new JoranConfigurator();
		configurator.setContext(context);
		// Call context.reset() to clear any previous configuration, e.g. default
		// configuration. For multi-step configuration, omit calling context.reset().
		context.reset();
		configurator.doConfigure("./src/main/resources/logback-consumer.xml");
		Consumer consumer = new Consumer();
		consumer.connect(new InetSocketAddress(InetAddress.getLocalHost(), Producer.DEFAULT_PORT));
		consumer.write(Producer.HELLO);
	}

}
