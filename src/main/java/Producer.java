
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

/**
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public class Producer {
	public static final int DEFAULT_PORT = 1234;
	private final NioSocketAcceptor acceptor = new NioSocketAcceptor();
	private final ProducerProtocolHandler producerProtocolHandler;
	private final Writer writer;

	private static Logger LOGGER = LoggerFactory.getLogger(Producer.class);

	public Producer(ProducerProtocolHandler producerProtocolHandler, Writer writer) {
		this.producerProtocolHandler = producerProtocolHandler; 
		this.writer = writer;
		this.acceptor.setCloseOnDeactivation(false);
		acceptor.setHandler(this.producerProtocolHandler);
		ProtocolCodecFilter fixCodecFilter = new ProtocolCodecFilter(new TextLineCodecFactory());
		DefaultIoFilterChainBuilder chain = acceptor.getFilterChain();
		chain.addLast("codec", fixCodecFilter);
	}

	public void bind(InetSocketAddress inetSocketAddress) throws IOException {
		this.acceptor.bind(inetSocketAddress);
	}
	
	public boolean awaitCompletion() throws InterruptedException, ExecutionException {
		//waiting for at least one connection
		this.producerProtocolHandler.awaitExpectedConnections();
		this.acceptor.unbind();
		this.producerProtocolHandler.awaitExpectedDataRequests();
		boolean overallSuccess = this.writer.awaitCompletion();
		this.producerProtocolHandler.awaitSessionsClosed();
		this.acceptor.dispose();
		LOGGER.info("Producer stopped.");
		return overallSuccess;
	}

	/**
	 * Run from root directory of the project
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, JoranException {
		LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
		JoranConfigurator configurator = new JoranConfigurator();
		configurator.setContext(context);
		// Call context.reset() to clear any previous configuration, e.g. default
		// configuration. For multi-step configuration, omit calling context.reset().
		context.reset();
		configurator.doConfigure("./src/main/resources/logback-producer.xml");		
		boolean isSuccessful = false;
		try {
			LOGGER.info("Starting.");
			int expectedConnections = ProducerProtocolHandler.DEFAULT_EXPECTED_CONNECTIONS;
			int threads = Writer.DEFAULT_PRODUCER_THREADS;
			int numberOfTasks = Writer.DEFAULT_NUMBER_TASKS;
			int messagesToSend = Writer.DEFAULT_MSG_COUNT;
			ExecutorService execService = Executors.newFixedThreadPool(threads);
			Writer writer = new Writer(numberOfTasks, messagesToSend, execService);
			writer.setAssertCorrectnessOfScheduledWriteMessages(false);
			LOGGER.info("Assert Correctness Of ScheduledWriteMessages : {}", writer.isAssertCorrectnessOfScheduledWriteMessages());
			
			ProducerProtocolHandler producerProtocolHandler = new ProducerProtocolHandler(writer, expectedConnections);
			
			Producer producer = new Producer(producerProtocolHandler, writer);
			producer.bind(new InetSocketAddress(DEFAULT_PORT));
			LOGGER.info("Listening on port {}", DEFAULT_PORT);
			
			isSuccessful = producer.awaitCompletion();
			execService.shutdown();

			long maxScheduledWriteBytes = writer.getMaxScheduledWriteBytes();
			LOGGER.info("Total Messages Sent        {}", writer.getSentMessages());
			LOGGER.info("maxScheduledWriteBytes     {} ", maxScheduledWriteBytes);
			LOGGER.info("meanScheduledWriteBytes    {}  ", maxScheduledWriteBytes/writer.getSentMessages());

			long maxScheduledWriteMessages = writer.getMaxScheduledWriteMessages();
			LOGGER.info("maxScheduledWriteMessages  {}", maxScheduledWriteMessages);
			LOGGER.info("meanScheduledWriteMessages {}", maxScheduledWriteMessages/writer.getSentMessages());
			
			long maxWriteRequestQueueSize = writer.getMaxWriteRequestQueueSize();
			LOGGER.info("maxWriteRequestQueueSize   {}",  maxWriteRequestQueueSize);
			LOGGER.info("meanWriteRequestQueueSize  {}",  maxWriteRequestQueueSize/writer.getSentMessages());
		} finally {
			LOGGER.info("Status [{}]" , isSuccessful == true ? "Success" :"Fail");
		}
	}
}
