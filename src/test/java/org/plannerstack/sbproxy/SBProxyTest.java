package org.plannerstack.sbproxy;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import org.jeromq.ZMQ;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.microsoft.windowsazure.services.serviceBus.ServiceBusContract;
import com.microsoft.windowsazure.services.serviceBus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.serviceBus.models.ReceiveSubscriptionMessageResult;

/**
 * Unit tests for the {@link SBProxy} class.
 */
@RunWith(PowerMockRunner.class)
public class SBProxyTest {

	ServiceBusContract azureService = mock(ServiceBusContract.class);
	ZMQ.Socket zmqSocket = mock(ZMQ.Socket.class);

	@Test
	public void testConvenienceConstructor() {
		SBProxy proxy = new SBProxy(azureService, zmqSocket);

		assertEquals(azureService, proxy.getAzureService());
		assertEquals(zmqSocket, proxy.getZmqSocket());
		assertEquals(1048576, proxy.getMaxMsgSize());
		assertEquals(true, proxy.usesCompression());
	}

	@Test
	public void testConstructor() {
		SBProxy proxy = new SBProxy(azureService, zmqSocket, 1337, false);

		assertEquals(azureService, proxy.getAzureService());
		assertEquals(zmqSocket, proxy.getZmqSocket());
		assertEquals(1337, proxy.getMaxMsgSize());
		assertEquals(false, proxy.usesCompression());
	}

	@Test
	@PrepareForTest(ZMQ.Socket.class)
	public void testProxyMessage() throws Exception {
		SBProxy proxy = new SBProxy(azureService, zmqSocket, 1024, false);

		String content = "message";
		BrokeredMessage message = new BrokeredMessage(content);
		message.setMessageId("messageId1");
		when(azureService.receiveSubscriptionMessage("topic", "subscription"))
				.thenReturn(new ReceiveSubscriptionMessageResult(message));

		proxy.proxyMessage("topic", "subscription");
		verify(zmqSocket).send(content.getBytes());
	}

	@Test
	@PrepareForTest(ZMQ.Socket.class)
	public void testSkipNullMessage() throws Exception {
		SBProxy proxy = new SBProxy(azureService, zmqSocket, 1024, false);

		when(azureService.receiveSubscriptionMessage("topic", "subscription"))
				.thenReturn(new ReceiveSubscriptionMessageResult(null));

		proxy.proxyMessage("topic", "subscription");
		verify(zmqSocket, never()).send(any(byte[].class));
	}

	@Test
	@PrepareForTest(ZMQ.Socket.class)
	public void testSkipNullMessageId() throws Exception {
		SBProxy proxy = new SBProxy(azureService, zmqSocket, 1024, false);

		String content = "message";
		BrokeredMessage message = new BrokeredMessage(content);
		when(azureService.receiveSubscriptionMessage("topic", "subscription"))
				.thenReturn(new ReceiveSubscriptionMessageResult(message));

		proxy.proxyMessage("topic", "subscription");
		verify(zmqSocket, never()).send(any(byte[].class));
	}

	@Test
	@PrepareForTest(ZMQ.Socket.class)
	public void testCompressedProxyMessage() throws Exception {
		SBProxy proxy = new SBProxy(azureService, zmqSocket, 1024, true);

		String content = "message2";
		BrokeredMessage message = new BrokeredMessage(content);
		message.setMessageId("messageId2");
		when(azureService.receiveSubscriptionMessage("topic", "subscription"))
				.thenReturn(new ReceiveSubscriptionMessageResult(message));

		proxy.proxyMessage("topic", "subscription");

		ByteArrayOutputStream expected = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(expected);
		gzip.write(content.getBytes());
		gzip.close();
		verify(zmqSocket).send(expected.toByteArray());
	}

	@Test(expected = IOException.class)
	public void testMessageSizeTooLong() throws Exception {
		ServiceBusContract azureService = mock(ServiceBusContract.class);
		ZMQ.Socket zmqSocket = mock(ZMQ.Socket.class);
		SBProxy proxy = new SBProxy(azureService, zmqSocket, 10, true);

		String content = "a message longer than 10 bytes";
		BrokeredMessage message = new BrokeredMessage(content);
		message.setMessageId("messageId3");
		when(azureService.receiveSubscriptionMessage("topic", "subscription"))
				.thenReturn(new ReceiveSubscriptionMessageResult(message));

		proxy.proxyMessage("topic", "subscription");
	}
}
