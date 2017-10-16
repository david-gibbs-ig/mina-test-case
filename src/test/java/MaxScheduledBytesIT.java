import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxScheduledBytesIT {
	
	private static Logger LOGGER = LoggerFactory.getLogger(MaxScheduledBytesIT.class);
	private Producer producer;
	private InetSocketAddress inetSocketAddress;
	
	@Before
	public void setUp() throws Exception {
		int messagesToSend = Producer.DEFAULT_MSG_COUNT;
		int producerThreads = Producer.DEFAULT_PRODUCER_THREADS;
		int expectedConnections = Producer.DEFAULT_EXPECTED_CONNECTIONS;
		producer = new Producer(messagesToSend, expectedConnections, producerThreads);
		inetSocketAddress = new InetSocketAddress(Producer.DEFAULT_PORT);
		producer.bind(inetSocketAddress);

	}

	@Test
	public void testCompletion() throws IOException, InterruptedException, ExecutionException {
		producer.setAssertCorrectnessOfScheduledWriteMessages(false);
		LOGGER.info("Assert Correctness Of ScheduledWriteMessages : {}", producer.isAssertCorrectnessOfScheduledWriteMessages());
		connectConsumerAndEvaluate();
	}
	
	@Test
	public void testCorrectnessOfScheduledWriteMessages() throws IOException, InterruptedException, ExecutionException {
		producer.setAssertCorrectnessOfScheduledWriteMessages(true);
		LOGGER.info("Assert Correctness Of ScheduledWriteMessages : {}", producer.isAssertCorrectnessOfScheduledWriteMessages());
		try {
			connectConsumerAndEvaluate();
		} catch (Exception e) {
			LOGGER.error("Exception in testCorrectnessOfScheduledWriteMessages",e);
			throw e;
		}
	}

	private void connectConsumerAndEvaluate() throws UnknownHostException, InterruptedException, ExecutionException {
		LOGGER.info("Listening on port {}", inetSocketAddress.getPort());
		Consumer consumer = new Consumer();
		consumer.connect(new InetSocketAddress(InetAddress.getLocalHost(), inetSocketAddress.getPort()));
		consumer.write(Producer.HELLO);
		boolean isSuccessful = producer.awaitCompletion();
		LOGGER.info("Status [{}]" , isSuccessful == true ? "Success" :"Fail");
		assertTrue(isSuccessful);
	}
}
