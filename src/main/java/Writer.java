import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Writer {
	
	private final int numberOfTasks;
	private final int messagesToSend;
	private final ExecutorService execService;

	protected boolean isAssertCorrectnessOfScheduledWriteMessages = false;

	private final LongAdder sumScheduledWriteBytes = new LongAdder();
	private final LongAdder sumScheduledWriteMessages = new LongAdder();
	private final LongAdder sumWriteRequestQueueSize = new LongAdder();
	private final LongAdder sumSentMessages = new LongAdder();
	
	protected final AtomicLong maxScheduledWriteBytes = new AtomicLong();
	protected final AtomicLong maxScheduledWriteMessages = new AtomicLong();
	protected final AtomicLong maxWriteRequestQueueSize = new AtomicLong();

	private static final Logger LOGGER = LoggerFactory.getLogger(Writer.class);

	private List<Future<Integer>> taskFutures = new ArrayList<Future<Integer>>();
	public static final int DEFAULT_PRODUCER_THREADS = 5;
	public static final int DEFAULT_MSG_COUNT = 100000;
	public static final int DEFAULT_NUMBER_TASKS = 3;
	
	public Writer(int numberOfTasks, int messagesToSend, ExecutorService execService) {
		this.numberOfTasks = numberOfTasks;
		this.messagesToSend = messagesToSend;
		this.execService = execService;
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
						for (; i < Writer.this.messagesToSend; ++i) {
							int msgNumber = i + 1;
							checkConditionsAndCollectMetrics(session, msgNumber);
							String payload = "Payload : [" + Integer.toString(taskId) + "] : " + Integer.toString(msgNumber);
							session.write(payload);
							Writer.this.sumSentMessages.increment();
						}
					} finally {
						countDownSessionTasks.countDown();
						if (countDownSessionTasks.getCount() == 0) {
							session.write(ProducerProtocolHandler.GOODBYE);
							Writer.this.sumSentMessages.increment();
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
					Writer.checkAndSetMaximum(Writer.this.maxScheduledWriteBytes,scheduledWriteBytes);
					Writer.checkAndSetMaximum(Writer.this.maxScheduledWriteMessages,scheduledWriteMessages);
					Writer.checkAndSetMaximum(Writer.this.maxWriteRequestQueueSize,writeRequestQueueSize);
					if (Writer.this.isAssertCorrectnessOfScheduledWriteMessages) {
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
					
					Writer.this.sumScheduledWriteBytes.add(scheduledWriteBytes);
					Writer.this.sumScheduledWriteMessages.add(scheduledWriteMessages);
					Writer.this.sumWriteRequestQueueSize.add(writeRequestQueueSize);
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
	
	private void submit(Callable<Integer> task) {
		LOGGER.debug("Adding task {} ", task);
		this.taskFutures.add(execService.submit(task));
	}
	
	public void setAssertCorrectnessOfScheduledWriteMessages(boolean isAssertCorrectnessOfScheduledWriteMessages) {
		this.isAssertCorrectnessOfScheduledWriteMessages = isAssertCorrectnessOfScheduledWriteMessages;
	}

	public boolean isAssertCorrectnessOfScheduledWriteMessages() {
		return isAssertCorrectnessOfScheduledWriteMessages;
	}

	public long getMaxScheduledWriteBytes() {
		return maxScheduledWriteBytes.get();
	}

	public long getMaxScheduledWriteMessages() {
		return maxScheduledWriteMessages.get();
	}

	public long getMaxWriteRequestQueueSize() {
		return maxWriteRequestQueueSize.get();
	}

	public int getMessagesToSend() {
		return messagesToSend;
	}

	public long getSentMessages() {
		return this.sumSentMessages.longValue();
	}
	
	public boolean awaitCompletion() throws InterruptedException, ExecutionException {
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
		return overallSuccess;
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

}
