
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public class Producer {
	public static final int DEFAULT_PORT = 1234;
	public static final int DEFAULT_MSG_COUNT = 100000;
	public static final int DEFAULT_PRODUCER_THREADS = 3;
	public static final int DEFAULT_EXPECTED_CONNECTIONS = 1;
	public static final String GOODBYE = "GoodBye";
	public static final String HELLO = "Hello";
	private final NioSocketAcceptor acceptor = new NioSocketAcceptor();
	private final ExecutorService execService;

	private ProducerProtocolHandler writer;
	private List<Future<Integer>> taskFutures = new ArrayList<Future<Integer>>();

	private final int messagesToSend;

	private LongAdder sumScheduledWriteBytes = new LongAdder();
	private LongAdder sumScheduledWriteMessages = new LongAdder();
	private LongAdder sumWriteRequestQueueSize = new LongAdder();
	private LongAdder sumSentMessages = new LongAdder();
	
	protected AtomicLong maxScheduledWriteBytes = new AtomicLong();
	protected AtomicLong maxScheduledWriteMessages = new AtomicLong();
	protected AtomicLong maxWriteRequestQueueSize = new AtomicLong();

	private static Logger LOGGER = LoggerFactory.getLogger(Producer.class);

	protected boolean isAssertCorrectnessOfScheduledWriteMessages = false;
	private int numberOfTasks;
	private final int expectedConnections;
	
	public Producer(int messagesToSend, int expectedConnections, int numberOfTasks) {
		this.messagesToSend = messagesToSend;
		//number of tasks (per request for data)
		this.numberOfTasks = numberOfTasks;
		//one thread per number of tasks
		this.execService = Executors.newFixedThreadPool(this.numberOfTasks);
		this.expectedConnections = expectedConnections;
		this.acceptor.setCloseOnDeactivation(false);
		ProtocolCodecFilter fixCodecFilter = new ProtocolCodecFilter(new TextLineCodecFactory());
		DefaultIoFilterChainBuilder chain = acceptor.getFilterChain();
		chain.addLast("codec", fixCodecFilter);
	}

	public boolean isAssertCorrectnessOfScheduledWriteMessages() {
		return isAssertCorrectnessOfScheduledWriteMessages;
	}

	public void setAssertCorrectnessOfScheduledWriteMessages(boolean isAssertCorrectnessOfScheduledWriteMessages) {
		this.isAssertCorrectnessOfScheduledWriteMessages = isAssertCorrectnessOfScheduledWriteMessages;
	}

	public int getMessagesToSend() {
		return messagesToSend;
	}

	private long getSentMessages() {
		return this.sumSentMessages.longValue();
	}

	public long getMaxWriteRequestQueueSize() {
		return maxWriteRequestQueueSize.get();
	}

	public long getMaxScheduledWriteMessages() {
		return maxScheduledWriteMessages.get();
	}

	public long getMaxScheduledWriteBytes() {
		return maxScheduledWriteBytes.get();
	}

	public void bind(InetSocketAddress inetSocketAddress) throws IOException {
		writer = new Producer.ProducerProtocolHandler(this, this.expectedConnections);
		acceptor.setHandler(writer);
		this.acceptor.bind(inetSocketAddress);
	}

	public void dataRequested(IoSession session) {
		final CountDownLatch countDownSessionTasks = new CountDownLatch(this.numberOfTasks);
		for (int i=0; i < this.numberOfTasks; ++i) {
			final int taskId = i;
			Callable<Integer> task = new Callable<Integer>() {
				private long lastScheduledWriteBytes = 0;
				private long lastScheduledWriteMessages = 0;
				private long lastWriteRequestQueue = 0;
				
				@Override
				public Integer call() {
					int i = 0;
					try {
						for (; i < Producer.this.messagesToSend; ++i) {
							int msgNumber = i + 1;
							checkConditionsAndCollectMetrics(session, msgNumber);
							String payload = "Payload : [" + Integer.toString(taskId) + "] : " + Integer.toString(msgNumber);
							session.write(payload);
							Producer.this.sumSentMessages.increment();
						}
					} finally {
						countDownSessionTasks.countDown();
						if (countDownSessionTasks.getCount() == 0) {
							session.write(Producer.GOODBYE);
							Producer.this.sumSentMessages.increment();
						}
					}
					return i;
				}

				private void checkConditionsAndCollectMetrics(IoSession session, int msgNumber) {
					LOGGER.debug("{} writing {} ", taskId, msgNumber);
					long scheduledWriteMessages = session.getScheduledWriteMessages();
					long scheduledWriteBytes = session.getScheduledWriteBytes();
					long writeRequestQueueSize = session.getWriteRequestQueue().size();
					LOGGER.debug("{} scheduled write messages {}", taskId, scheduledWriteMessages);
					LOGGER.debug("{} scheduled write bytes {}", taskId, scheduledWriteBytes);
					LOGGER.debug("{} scheduled write request queue size {}", taskId, writeRequestQueueSize);
					Producer.checkAndSetMaximum(Producer.this.maxScheduledWriteBytes,scheduledWriteBytes);
					Producer.checkAndSetMaximum(Producer.this.maxScheduledWriteMessages,scheduledWriteMessages);
					Producer.checkAndSetMaximum(Producer.this.maxWriteRequestQueueSize,writeRequestQueueSize);
					if (Producer.this.isAssertCorrectnessOfScheduledWriteMessages) {
						throwIfSizeLessThanZero(scheduledWriteMessages, "ScheduledWriteMessages");
					}
					throwIfSizeLessThanZero(scheduledWriteBytes,   "ScheduledWriteBytes");
					throwIfSizeLessThanZero(writeRequestQueueSize, "WriteRequestQueueSize");
					if (scheduledWriteBytes < this.lastScheduledWriteBytes) {
						LOGGER.debug("{} Msg {} Buffer written.", taskId, msgNumber);
						LOGGER.debug("{} scheduledWriteBytes {} < last {} [{}]",
								taskId,
								scheduledWriteBytes,
								this.lastScheduledWriteBytes,
								this.lastScheduledWriteBytes - scheduledWriteBytes); 
						LOGGER.debug("{} scheduledWriteMessages {} < last {} [{}]",
								taskId,
								scheduledWriteMessages,
								this.lastScheduledWriteMessages,
								this.lastScheduledWriteMessages - scheduledWriteMessages) ;
					}
					if (writeRequestQueueSize < this.lastWriteRequestQueue) {
						LOGGER.debug("{} Msg {} RequestQueue written.", taskId,  msgNumber);
						LOGGER.debug("{} writeRequestQueueSize {} < last {} [{}]",
								taskId,
								writeRequestQueueSize,
								this.lastWriteRequestQueue,
								this.lastWriteRequestQueue - writeRequestQueueSize) ;
					}
					this.lastScheduledWriteBytes = scheduledWriteBytes;
					this.lastScheduledWriteMessages = scheduledWriteMessages;
					this.lastWriteRequestQueue = writeRequestQueueSize;
					
					Producer.this.sumScheduledWriteBytes.add(scheduledWriteBytes);
					Producer.this.sumScheduledWriteMessages.add(scheduledWriteMessages);
					Producer.this.sumWriteRequestQueueSize.add(writeRequestQueueSize);
				}

				private void throwIfSizeLessThanZero(long value, String valueName) {
					if (value < 0) {
						throw new ConditionFailedException(valueName + " less than 0.");
					}
				}
			};
			submit(task);
		}
	}

	private static void checkAndSetMaximum(AtomicLong currentMaximum, long valueToCompare) {
		long l = currentMaximum.get();
		if (valueToCompare > l) {
			if (!currentMaximum.compareAndSet(l,valueToCompare)) {
				// if compareAndSet returns false the current value has changed, compare again
				checkAndSetMaximum(currentMaximum, valueToCompare);
			};
		}
	}
	
	private void submit(Callable<Integer> task) {
		LOGGER.debug("Adding task {} ", task);
		this.taskFutures.add(execService.submit(task));
	}

	public boolean awaitCompletion() throws InterruptedException, ExecutionException {
		//waiting for at least one connection
		this.writer.awaitExpectedConnections();
		this.acceptor.unbind();
		this.writer.awaitExpectedDataRequests();
		boolean overallSuccess = true;
		for (Future<Integer> task: this.taskFutures) {
		    try {
				Integer messagesSent = task.get();
				boolean result = messagesSent.intValue() == this.messagesToSend;
			    LOGGER.info("Task {} completed : [{}]. total messages sent {}, data messages specified {}", 
			    		task,
			    		result == true ? "Success" :"Fail",
			    		messagesSent,  
			    		this.messagesToSend);
			    if (result == false) {
			    	overallSuccess = false;
			    }
		    } catch (Exception e) {
		    	LOGGER.error("Exception",e);
		    	throw e;
		    }
		}
		this.writer.awaitSessionsClosed();
		this.execService.shutdown();
		this.acceptor.dispose();
		LOGGER.info("Producer stopped.");
		return overallSuccess;
	}

	static class ProducerProtocolHandler extends IoHandlerAdapter {
		private final Set<IoSession> sessions = new HashSet<IoSession>();
		private final Producer producer;
		private final Lock lock = new ReentrantLock();
		private final Condition isSessionsEmptyCondition = lock.newCondition();
		private final CountDownLatch countDownConnections; 
		private final CountDownLatch countDownDataRequests; 
		
		ProducerProtocolHandler(Producer producer, int expectedConnections) {
			this.producer = producer;
			this.countDownConnections = new CountDownLatch(expectedConnections);
			this.countDownDataRequests = new CountDownLatch(expectedConnections);
		}

		@Override
		public void exceptionCaught(IoSession session, Throwable cause) {
			LOGGER.error("Unexpected throwable", cause);
			// Close connection when unexpected exception is caught.
			session.closeNow();
			removeSession(session);
		}

		@Override
		public void messageReceived(IoSession session, Object message) {
			if (message.toString().equals(HELLO)) {
				LOGGER.info("{} from {}", HELLO, session);
				this.producer.dataRequested(session);
				this.countDownDataRequests.countDown();
			} else if (message.toString().equals(GOODBYE)) {
				LOGGER.info("{} from {}", GOODBYE, session);
				closeSession(session);
			} else {
				LOGGER.error("{} from {}", message, session);
			}
			LOGGER.info("received: {} ", message.toString());
		}

		@Override
		public void sessionCreated(IoSession session) throws Exception {
			processNewSession(session);
			super.sessionCreated(session);
		}

		@Override
		public void sessionClosed(IoSession session) throws Exception {
			LOGGER.info("session closed.");
			removeSession(session);
		}

		private void processNewSession(IoSession session) {
			boolean isSessionNew;
			try {
				this.lock.lock();
				isSessionNew = this.sessions.add(session);
				this.countDownConnections.countDown();
			} finally {
				this.lock.unlock();
			}
			if (isSessionNew) {
				LOGGER.info("Added new session {}.", session);
			} else { 
				LOGGER.warn("Session {} already exists.", session);
			}
		}

		private void removeSession(IoSession session) {
			boolean isSessionExtant;
			try {
				this.lock.lock();
				isSessionExtant = this.sessions.remove(session);
				if (this.sessions.isEmpty()) {
					this.isSessionsEmptyCondition.signal();
				}
			} finally {
				this.lock.unlock();
			}
			if (isSessionExtant) {
				LOGGER.info("Removed session {}.", session);
			} else {
				LOGGER.warn("Session {} did not exist.", session);
			}
		}
		
		private void closeSession(IoSession session) {
			session.closeNow();
			removeSession(session);
		}

		public void awaitSessionsClosed() throws InterruptedException {
			while (true) {
				try {
					this.lock.lock();
					if (this.sessions.isEmpty()) {
						break;
					} else {
						this.isSessionsEmptyCondition.await();
					}
				} finally {
					this.lock.unlock();
				}
			}
		}

		public void awaitExpectedConnections() throws InterruptedException {
			this.countDownConnections.await();
		}

		public void awaitExpectedDataRequests() throws InterruptedException {
			this.countDownDataRequests.await();
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		boolean isSuccessful = false;
		try {
			int tasks = Producer.DEFAULT_PRODUCER_THREADS;
			int msgCount = Producer.DEFAULT_MSG_COUNT;
			int expectedConnections = Producer.DEFAULT_EXPECTED_CONNECTIONS;
			Producer producer = new Producer(msgCount,expectedConnections,tasks);
			producer.setAssertCorrectnessOfScheduledWriteMessages(false);
			LOGGER.info("Assert Correctness Of ScheduledWriteMessages : {}", producer.isAssertCorrectnessOfScheduledWriteMessages());
			producer.bind(new InetSocketAddress(DEFAULT_PORT));
			LOGGER.info("Listening on port {}", DEFAULT_PORT);
			isSuccessful = producer.awaitCompletion();
			long maxScheduledWriteBytes = producer.getMaxScheduledWriteBytes();
			LOGGER.info("Total Messages Sent        {}", producer.getSentMessages());
			LOGGER.info("maxScheduledWriteBytes     {} ", maxScheduledWriteBytes);
			LOGGER.info("meanScheduledWriteBytes    {}  ", maxScheduledWriteBytes/producer.getSentMessages());

			long maxScheduledWriteMessages = producer.getMaxScheduledWriteMessages();
			LOGGER.info("maxScheduledWriteMessages  {}", maxScheduledWriteMessages);
			LOGGER.info("meanScheduledWriteMessages {}", maxScheduledWriteMessages/producer.getSentMessages());
			
			long maxWriteRequestQueueSize = producer.getMaxWriteRequestQueueSize();
			LOGGER.info("maxWriteRequestQueueSize   {}",  maxWriteRequestQueueSize);
			LOGGER.info("meanWriteRequestQueueSize  {}",  maxWriteRequestQueueSize/producer.getSentMessages());
		} finally {
			LOGGER.info("Status [{}]" , isSuccessful == true ? "Success" :"Fail");
		}
	}
}
