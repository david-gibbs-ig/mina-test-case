import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledBytesAndMessagesIT {
	
	private static Logger LOGGER = LoggerFactory.getLogger(ScheduledBytesAndMessagesIT.class);
	private Writer writer;
	private Producer producer;
	private ExecutorService execService;
	private InetSocketAddress inetSocketAddress;
	private int expectedConnections;
	
	@Before
	public void setUp() throws Exception {
		expectedConnections = 3;
		int threads = Writer.DEFAULT_PRODUCER_THREADS;
		int messagesToSend = Writer.DEFAULT_MSG_COUNT;
		int numberOfTasks = Writer.DEFAULT_NUMBER_TASKS;
		execService = Executors.newFixedThreadPool(threads);
		writer = new Writer(numberOfTasks, messagesToSend, execService);
		ProducerProtocolHandler producerProtocolHandler = new ProducerProtocolHandler(writer, expectedConnections);
		
		producer = new Producer(producerProtocolHandler, writer);
		inetSocketAddress = new InetSocketAddress(Producer.DEFAULT_PORT);
		producer.bind(inetSocketAddress);
	}
	
	@After
	public void shutdown() {
		execService.shutdown();
	}

	@Test
	public void testCompletion() throws IOException, InterruptedException, ExecutionException {
		writer.setAssertCorrectnessOfScheduledWriteMessages(false);
		LOGGER.info("Assert Correctness Of ScheduledWriteMessages : {}", writer.isAssertCorrectnessOfScheduledWriteMessages());
		connectConsumerAndEvaluate();
	}
	
	@Test
	public void testCorrectnessOfScheduledWriteMessages() throws IOException, InterruptedException, ExecutionException {
		writer.setAssertCorrectnessOfScheduledWriteMessages(true);
		LOGGER.info("Assert Correctness Of ScheduledWriteMessages : {}", writer.isAssertCorrectnessOfScheduledWriteMessages());
		try {
			connectConsumerAndEvaluate();
		} catch (Exception e) {
			LOGGER.error("Exception in testCorrectnessOfScheduledWriteMessages",e);
			throw e;
		}
	}

	private void connectConsumerAndEvaluate() throws UnknownHostException, InterruptedException, ExecutionException {
		LOGGER.info("Listening on port {}", inetSocketAddress.getPort());
		for (int i = 0; i < this.expectedConnections; ++i) {
			Consumer consumer = new Consumer();
			consumer.connect(new InetSocketAddress(InetAddress.getLocalHost(), inetSocketAddress.getPort()));
			consumer.write(ProducerProtocolHandler.HELLO);
		}
		boolean isSuccessful = producer.awaitCompletion();
		LOGGER.info("Status [{}]" , isSuccessful == true ? "Success" :"Fail");
		assertTrue(isSuccessful);
	}
}
