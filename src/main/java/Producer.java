
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.core.future.CloseFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import quickfix.field.Headline;
import quickfix.fix44.News;
import quickfix.mina.message.FIXProtocolCodecFactory;

/**
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public class Producer {
	public static final int DEFAULT_PORT = 1234;
	public static final int DEFAULT_MSG_COUNT = 100000;
	private final NioSocketAcceptor acceptor = new NioSocketAcceptor();
	private final ExecutorService execService = Executors.newSingleThreadExecutor();

	private CountDownLatch countDownLatch;
	private ProducerProtocolHandler writer;
	private Future<Boolean> taskFuture;

	private final int messagesToSend;

	private long sumScheduledWriteBytes = 0;
	private long sumScheduledWriteMessages = 0;
	private long sumWriteRequestQueueSize = 0;

	protected long maxScheduledWriteBytes = 0;
	protected long maxScheduledWriteMessages = 0;
	protected long maxWriteRequestQueueSize = 0;

	protected double meanScheduledWriteBytes = 0;
	protected double meanScheduledWriteMessages = 0;
	protected double meanWriteRequestQueueSize = 0;
	
	private static Logger LOGGER = LoggerFactory.getLogger(Producer.class);

	protected boolean isBufferWritten = false;

	public Producer(int messagesToSend) {
		this.messagesToSend = messagesToSend;
		ProtocolCodecFilter fixCodecFilter = new ProtocolCodecFilter(new FIXProtocolCodecFactory());

		DefaultIoFilterChainBuilder chain = acceptor.getFilterChain();
		chain.addLast("codec", fixCodecFilter);
		//chain.addLast("logger", new LoggingFilter());
		countDownLatch = new CountDownLatch(this.messagesToSend);
	}

	public int getMessagesToSend() {
		return messagesToSend;
	}

	public void bind(InetSocketAddress inetSocketAddress) throws IOException {
		writer = new Producer.ProducerProtocolHandler(this);
		acceptor.setHandler(writer);
		this.acceptor.bind(inetSocketAddress);
	}

	static class ProducerProtocolHandler extends IoHandlerAdapter {
		private final Set<IoSession> sessions = Collections.synchronizedSet(new HashSet<IoSession>());
		private final Producer producer;

		ProducerProtocolHandler(Producer producer) {
			this.producer = producer;
		}

		@Override
		public void exceptionCaught(IoSession session, Throwable cause) {
			LOGGER.error("Unexpected throwable", cause);
			// Close connection when unexpected exception is caught.
			session.closeNow();
		}

		@Override
		public void messageReceived(IoSession session, Object message) {
			LOGGER.info("received: {} ", message.toString());
			sessions.add(session);
			this.producer.messageReceived(session, message);
		}

		@Override
		public void sessionClosed(IoSession session) throws Exception {
			LOGGER.info("session closed.");
			sessions.remove(session);
		}

		public Set<IoSession> getSessions() {
			return sessions;
		}
	}

	public boolean awaitCompletion(long timeout, TimeUnit timeUnit) throws InterruptedException {
		return this.countDownLatch.await(timeout, timeUnit);
	}

	public void messageReceived(IoSession session, Object message) {
		Callable<Boolean> task = new Callable<Boolean>() {
			long lastScheduledWriteBytes = 0;
			long lastScheduledWriteMessages = 0;
			long lastWriteRequestQueue = 0;
			
			@Override
			public Boolean call() {
				int i = 0;
				try {
					for (; i < Producer.this.messagesToSend; ++i) {
						int msgNumber = i + 1;
						checkConditionsAndCollectMetrics(session, msgNumber);
						News news = new News();
						news.set(new Headline("Headline : " + Integer.toString(msgNumber)));
						session.write(news);
						Producer.this.countDownLatch.countDown();
					}
				} catch (Exception e) {
					LOGGER.error("Exception " + e.getClass().getName());
					e.printStackTrace();
				} finally {
					session.closeOnFlush();
				}
				Producer.this.meanScheduledWriteBytes = Producer.this.sumScheduledWriteBytes / i;
				Producer.this.meanScheduledWriteMessages = Producer.this.sumScheduledWriteMessages / i;
				Producer.this.meanWriteRequestQueueSize = Producer.this.sumWriteRequestQueueSize / i;
				return (i == Producer.this.messagesToSend);
			}

			private void checkConditionsAndCollectMetrics(IoSession session, int msgNumber) {
				LOGGER.debug("in writer : ");
				LOGGER.debug("in writer : writing " + msgNumber);
				long scheduledWriteMessages = session.getScheduledWriteMessages();
				LOGGER.debug("scheduled write messages " + scheduledWriteMessages);
				long scheduledWriteBytes = session.getScheduledWriteBytes();
				LOGGER.debug("scheduled write bytes " + scheduledWriteBytes);
				long writeRequestQueueSize = session.getWriteRequestQueue().size();
				LOGGER.debug("scheduled write request queue size " + writeRequestQueueSize);
				if (scheduledWriteBytes > Producer.this.maxScheduledWriteBytes) {
					Producer.this.maxScheduledWriteBytes = scheduledWriteBytes;
				}
				if (scheduledWriteMessages > Producer.this.maxScheduledWriteMessages) {
					Producer.this.maxScheduledWriteMessages = scheduledWriteMessages;
				}
				if (writeRequestQueueSize > Producer.this.maxWriteRequestQueueSize) {
					Producer.this.maxWriteRequestQueueSize = writeRequestQueueSize;
				}
				//throwIfSizeLessThanZero(scheduledWriteMessages, "ScheduledWriteMessages");
				throwIfSizeLessThanZero(scheduledWriteBytes, "ScheduledWriteBytes");
				throwIfSizeLessThanZero(writeRequestQueueSize, "WriteRequestQueueSize");
				if (scheduledWriteBytes < this.lastScheduledWriteBytes) {
					LOGGER.info("Msg " + msgNumber + " Buffer written.");
					LOGGER.info("scheduledWriteBytes "
							+ scheduledWriteBytes + " < last " + this.lastScheduledWriteBytes + 
							" [" + (this.lastScheduledWriteBytes - scheduledWriteBytes) + "]") ;
					LOGGER.info("scheduledWriteMessages " + scheduledWriteMessages + "    last " + this.lastScheduledWriteMessages + 
							" [" + (this.lastScheduledWriteMessages - scheduledWriteMessages) + "]") ;
				}
				if (writeRequestQueueSize < this.lastWriteRequestQueue) {
					LOGGER.info("Msg " + msgNumber + " RequestQueue written.");
					LOGGER.info("writeRequestQueueSize "  + writeRequestQueueSize  + "    last " + this.lastWriteRequestQueue + 
							" [" + (this.lastWriteRequestQueue - writeRequestQueueSize) + "]") ;
					Producer.this.isBufferWritten = true;
				}
				this.lastScheduledWriteBytes = scheduledWriteBytes;
				this.lastScheduledWriteMessages = scheduledWriteMessages;
				this.lastWriteRequestQueue = writeRequestQueueSize;
				
				Producer.this.sumScheduledWriteBytes = Producer.this.sumScheduledWriteBytes + scheduledWriteBytes;
				Producer.this.sumScheduledWriteMessages = Producer.this.sumScheduledWriteMessages + scheduledWriteMessages;
				Producer.this.sumWriteRequestQueueSize = Producer.this.sumWriteRequestQueueSize + writeRequestQueueSize;
			}
			
			private void throwIfSizeLessThanZero(long value, String valueName) {
				if (value < 0) {
					throw new ConditionFailedException(valueName + " less than 0.");
				}
			}
			

		};
		submit(task);
	}

	public void submit(Callable<Boolean> task) {
		this.taskFuture = execService.submit(task);
	}

	public Future<Boolean> getTaskFuture() {
		return taskFuture;
	}

	public void stop() throws InterruptedException {
		for (IoSession ioSession : writer.getSessions()) {
			CloseFuture closeOnFlush = ioSession.closeOnFlush();
			closeOnFlush.await();
		}
		this.execService.shutdown();
		this.acceptor.unbind();
		this.acceptor.dispose();
		LOGGER.info("Producer stopped.");
	}

	public boolean isBufferWritten() {
		return this.isBufferWritten;
	}

	public static void main(String[] args) {
		int deadLine = 60;
		TimeUnit deadLineTimeUnit = TimeUnit.SECONDS;
		boolean isTaskSuccessful = false;
		try {
			Producer producer = new Producer(Producer.DEFAULT_MSG_COUNT);
			producer.bind(new InetSocketAddress(DEFAULT_PORT));
			LOGGER.info("Listening on port " + DEFAULT_PORT);
			boolean isCompleted = producer.awaitCompletion(deadLine, deadLineTimeUnit);
			LOGGER.info("Is Completed ? " + isCompleted);
			producer.stop();
			Boolean result = producer.getTaskFuture().get();
			isTaskSuccessful = result.booleanValue();
			LOGGER.info("maxScheduledWriteBytes    " + producer.maxScheduledWriteBytes);
			LOGGER.info("meanScheduledWriteBytes    " + producer.meanScheduledWriteBytes);

			LOGGER.info("maxScheduledWriteMessages " + producer.maxScheduledWriteMessages);
			LOGGER.info("meanScheduledWriteMessages " + producer.meanScheduledWriteMessages);

			LOGGER.info("maxWriteRequestQueueSize  " + producer.maxWriteRequestQueueSize);
			LOGGER.info("meanWriteRequestQueueSize  " + producer.meanWriteRequestQueueSize);
		} catch (InterruptedException | ExecutionException | IOException e) {
			LOGGER.info("Exception " + e.getClass().getName());
			e.printStackTrace();
		} finally {
			LOGGER.info("Is Task Successful " + isTaskSuccessful);
		}
	}
}
