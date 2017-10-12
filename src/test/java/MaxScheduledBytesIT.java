import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import quickfix.field.Headline;
import quickfix.fix44.News;

public class MaxScheduledBytesIT {
	
	private static Logger LOGGER = LoggerFactory.getLogger(MaxScheduledBytesIT.class);
	
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testCompletion() throws IOException, InterruptedException, ExecutionException {
		int deadLine = 60;
		TimeUnit deadLineTimeUnit = TimeUnit.SECONDS;
		Producer producer = new Producer(Producer.DEFAULT_MSG_COUNT);
		InetSocketAddress inetSocketAddress = new InetSocketAddress(Producer.DEFAULT_PORT);
		producer.bind(inetSocketAddress);
		LOGGER.info("Listening on port " + inetSocketAddress.getPort());
		Consumer consumer = new Consumer();
		consumer.connect(new InetSocketAddress(InetAddress.getLocalHost(), inetSocketAddress.getPort()));
		News news = new News();
		news.set(new Headline("headline"));
		consumer.write(news);
		boolean isCompleted = producer.awaitCompletion(deadLine, deadLineTimeUnit);
		assertTrue(isCompleted);
		assertTrue(producer.isBufferWritten());
		producer.stop();
		Boolean result = producer.getTaskFuture().get();
		boolean isTaskSuccessful = result.booleanValue();
		assertTrue(isTaskSuccessful);
	}
}
