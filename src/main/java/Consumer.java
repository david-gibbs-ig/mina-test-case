
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.filter.logging.MdcInjectionFilter;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import quickfix.field.Headline;
import quickfix.fix44.News;
import quickfix.mina.message.FIXProtocolCodecFactory;

/**
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public class Consumer {
	private final NioSocketConnector connector = new NioSocketConnector();

	public Consumer() {
		ConsumerProtocolHandler consumer = new Consumer.ConsumerProtocolHandler();

		ProtocolCodecFilter fixCodecFilter = new ProtocolCodecFilter(new FIXProtocolCodecFactory());

		connector.getFilterChain().addLast("mdc", new MdcInjectionFilter());
		connector.getFilterChain().addLast("codec", fixCodecFilter);
		connector.getFilterChain().addLast("logger", new LoggingFilter());

		connector.setHandler(consumer);
	}

	public void write(News news) {
		for (IoSession session : this.connector.getManagedSessions().values()) {
			session.write(news);
		}
	}

	private boolean connect(InetSocketAddress inetSocketAddress) throws UnknownHostException {
		ConnectFuture future1 = connector.connect(inetSocketAddress);
		future1.awaitUninterruptibly();
		System.out.println("done waiting");
		if (future1.isConnected()) {
			return true;
		} else {
			return false;
		}
	}

	static class ConsumerProtocolHandler extends IoHandlerAdapter {
		private final Set<IoSession> sessions = Collections.synchronizedSet(new HashSet<IoSession>());

		@Override
		public void exceptionCaught(IoSession session, Throwable cause) {
			System.out.println("Unexpected exception." + cause);
			// Close connection when unexpected exception is caught.
			session.closeNow();
		}

		@Override
		public void messageReceived(IoSession session, Object message) {
			System.out.println("received: " + message);
			sessions.add(session);
		}

		@Override
		public void sessionClosed(IoSession session) throws Exception {
			sessions.remove(session);
		}
	}

	public static void main(String[] args) throws Exception {
		Consumer consumer = new Consumer();
		consumer.connect(new InetSocketAddress(InetAddress.getLocalHost(), Producer.DEFAULT_PORT));

		News news = new News();
		news.set(new Headline("headline"));
		consumer.write(news);
	}

}
