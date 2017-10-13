import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxScheduledBytesIT {
	
	private static Logger LOGGER = LoggerFactory.getLogger(MaxScheduledBytesIT.class);
	private Producer producer;
	private int deadLine;
	private TimeUnit deadLineTimeUnit;
	private int messagesToSend;
	private InetSocketAddress inetSocketAddress;
	
	@Before
	public void setUp() throws Exception {
		deadLine = 60;
		deadLineTimeUnit = TimeUnit.SECONDS;
		messagesToSend = Producer.DEFAULT_MSG_COUNT;
		producer = new Producer(messagesToSend);
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
		connectConsumerAndEvaluate();
	}

	private void connectConsumerAndEvaluate() throws UnknownHostException, InterruptedException, ExecutionException {
		LOGGER.info("Listening on port {}", inetSocketAddress.getPort());
		Consumer consumer = new Consumer();
		consumer.connect(new InetSocketAddress(InetAddress.getLocalHost(), inetSocketAddress.getPort()));
		consumer.write("headline");
		boolean isCompleted = producer.awaitCompletion(deadLine, deadLineTimeUnit);
		LOGGER.info("Is Completed ? [{}]", isCompleted);
		boolean isSuccessful = producer.stop();
		LOGGER.info("Status [{}]" , isSuccessful == true ? "Success" :"Fail");
		assertTrue(isCompleted);
		producer.stop();
		boolean isTaskSuccessful = producer.stop();;
		assertTrue(isTaskSuccessful);
	}
}
