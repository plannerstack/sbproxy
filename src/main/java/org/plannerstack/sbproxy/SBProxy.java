package org.plannerstack.sbproxy;

import static com.google.common.io.ByteStreams.copy;
import static com.google.common.io.ByteStreams.limit;
import static com.google.common.io.ByteStreams.toByteArray;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.windowsazure.services.core.Configuration;
import com.microsoft.windowsazure.services.core.ServiceException;
import com.microsoft.windowsazure.services.serviceBus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.serviceBus.ServiceBusContract;
import com.microsoft.windowsazure.services.serviceBus.ServiceBusService;
import com.microsoft.windowsazure.services.serviceBus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.serviceBus.models.ReceiveSubscriptionMessageResult;

/**
 * Azure Service Bus to ZeroMQ proxy.
 * <p>
 * The proxy listens for incoming messages on an Azure Service Bus topic and
 * forwards all messages on a ZeroMQ socket, optionally compressing them using
 * GZIP.
 */
public class SBProxy {
	private static final Logger log = LoggerFactory.getLogger(SBProxy.class);

	/** The default maximum message size. */
	public static final int DEFAULT_MAX_MSG_SIZE = 1 * 1024 * 1024; // in bytes

	private static final String AZURE_SERVICE_BUS_ROOT_URI = ".servicebus.windows.net";
	private static final String AZURE_WRAP_ROOT_URI = "-sb.accesscontrol.windows.net/WRAPv0.9";
	private static final long ZMQ_LINGER = 1000; // in milliseconds

	private ServiceBusContract azureService;
	private Socket zmqSocket;
	private int maxMsgSize;
	private boolean useCompression;

	/**
	 * Create an Azure Service Bus to ZeroMQ proxy with the default maximum
	 * message size and compressing outgoing messages.
	 *
	 * @param azureService the Azure Service Bus contract to receive messages
	 *            from
	 * @param zmqSocket the ZeroMQ socket to forward messages to; must already
	 *            have been bound to an interface
	 * @see #DEFAULT_MAX_MSG_SIZE
	 */
	public SBProxy(ServiceBusContract azureService, Socket zmqSocket) {
		this(azureService, zmqSocket, DEFAULT_MAX_MSG_SIZE, true);
	}

	/**
	 * Create an Azure Service Bus to ZeroMQ proxy.
	 *
	 * @param azureService the Azure Service Bus contract to receive messages
	 *            from
	 * @param zmqSocket the ZeroMQ socket to forward messages to; must already
	 *            have been bound to an interface
	 * @param maxMsgSize the maximum message size (in bytes)
	 * @param useCompression compress outgoing messages using GZIP
	 */
	public SBProxy(ServiceBusContract azureService, Socket zmqSocket, int maxMsgSize, boolean useCompression) {
		this.azureService = azureService;
		this.zmqSocket = zmqSocket;
		this.maxMsgSize = maxMsgSize;
		this.useCompression = useCompression;
	}

	/**
	 * Create an Azure Service Bus contract using the default Azure root URIs.
	 *
	 * @param azureConfig the Service Bus configuration
	 * @return the Service Bus contract
	 * @see <a href="http://www.windowsazure.com/en-us/develop/java/how-to-guides/service-bus-topics/">the
	 *      Azure Service Bus documentation for Java</a>
	 */
	public static ServiceBusContract createAzureService(String namespace, String authname, String password) {
		Configuration config = ServiceBusConfiguration.configureWithWrapAuthentication(namespace, authname, password,
				AZURE_SERVICE_BUS_ROOT_URI, AZURE_WRAP_ROOT_URI);
		log.info("creating Azure Service Bus service for URI: {}", config.getProperty(ServiceBusConfiguration.URI));
		return ServiceBusService.create(config);
	}

	/**
	 * Create a ZeroMQ socket and bind it to an network interface.
	 *
	 * @param zmqAddress the address to bind the socket to
	 * @return the ZeroMQ socket; remember to close the socket
	 * @see <a href="http://zguide.zeromq.org/page:all">the ZeroMQ guide</a>
	 */
	public static Socket createZmqSocket(String zmqAddress) {
		log.info("creating ZeroMQ endpoint: {}", zmqAddress);
		ZMQ.Context context = ZMQ.context();
		Socket socket = context.socket(ZMQ.PUB);
		socket.bind(zmqAddress);
		socket.setLinger(ZMQ_LINGER);
		return socket;
	}

	/**
	 * Start the main proxy loop. Receive messages from the Azure Service Bus
	 * and forward them to the ZeroMQ socket. This method will never return and
	 * consume all exceptions.
	 *
	 * @param topic the topic path
	 * @param subscription the subscription
	 * @see #proxyMessage
	 */
	public void proxyLoop(String topic, String subscription) {
		while (true) {
			try {
				proxyMessage(topic, subscription);
			} catch (Exception e) {
				log.error("error in proxy loop", e);
				break;
			}
		}
	}

	/**
	 * Receive a single message from the Azure Service Bus and forward it to the
	 * ZeroMQ socket.
	 *
	 * @param topic the topic path
	 * @param subscription the subscription
	 * @throws ServiceException if there is an error with the Azure Service Bus
	 * @throws IOException if there is an I/O error
	 */
	public void proxyMessage(String topic, String subscription) throws ServiceException, IOException {
		log.debug("receiving subscription message for topic: {}", topic);
		ReceiveSubscriptionMessageResult result = azureService.receiveSubscriptionMessage(topic, subscription);
		BrokeredMessage msg = result.getValue();
		if (msg == null) {
			log.warn("received null message");
			return;
		}
		if (msg.getMessageId() == null) {
			log.warn("received message with null id: {}", msg.getLabel());
			return;
		}

		log.debug("received message id: {}", msg.getMessageId());
		byte[] output;
		if (useCompression) {
			ByteArrayOutputStream compress = new ByteArrayOutputStream();
			gzipCompress(msg.getBody(), compress);
			output = compress.toByteArray();
		} else {
			output = toByteArray(msg.getBody());
		}
		zmqSocket.send(output);
	}

	private void gzipCompress(InputStream is, OutputStream os) throws IOException {
		GZIPOutputStream gzip = new GZIPOutputStream(os);
		copy(limit(is, maxMsgSize), gzip);
		gzip.finish();
		if (is.read() != -1) throw new IOException("message too long");
	}

	/**
	 * @return the Azure service contract
	 */
	public ServiceBusContract getAzureService() {
		return azureService;
	}

	/**
	 * @return the ZeroMQ socket
	 */
	public Socket getZmqSocket() {
		return zmqSocket;
	}

	/**
	 * @return the maximum message size
	 */
	public int getMaxMsgSize() {
		return maxMsgSize;
	}

	/**
	 * @return whether message compression is enabled
	 */
	public boolean usesCompression() {
		return useCompression;
	}
}
