import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerProtocolHandler extends IoHandlerAdapter {
	public static final String GOODBYE = "GoodBye";
	public static final String HELLO = "Hello";

	private final Set<IoSession> sessions = new HashSet<IoSession>();
	private final Lock lock = new ReentrantLock();
	private final Condition isSessionsEmptyCondition = lock.newCondition();
	private final CountDownLatch countDownConnections;
	private final CountDownLatch countDownDataRequests;
	private final Writer writer;
	public static final int DEFAULT_EXPECTED_CONNECTIONS = 1;

	private static final Logger LOGGER = LoggerFactory.getLogger(Writer.class);

	public ProducerProtocolHandler(Writer writer, int expectedConnections) {
		this.writer = writer;
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
			this.writer.dataRequested(session);
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