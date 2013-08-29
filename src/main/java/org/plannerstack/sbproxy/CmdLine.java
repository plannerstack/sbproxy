package org.plannerstack.sbproxy;

import org.jeromq.ZMQ;
import org.kohsuke.args4j.CmdLineException;

import com.microsoft.windowsazure.services.serviceBus.ServiceBusContract;

/**
 * Azure Service Bus to ZeroMQ proxy command line tool.
 */
public class CmdLine {

	/**
	 * The proxy command line main method.
	 */
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		try {
			options.parse(args);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			options.printUsage(System.err);
			return;
		}
		new CmdLine().run(options);
	}

	/**
	 * Run the proxy.
	 */
	public void run(Options options) {
		ServiceBusContract azureService = SBProxy.createAzureService(options.azureNamespace, options.azureAuthname,
				options.azurePassword);
		ZMQ.Socket zmqSocket = SBProxy.createZmqSocket(options.zmqAddress);
		SBProxy proxy = new SBProxy(azureService, zmqSocket, options.maxMsgSize, options.zmqCompression);
		try {
			proxy.proxyLoop(options.azureTopic, options.azureSubscription);
		} finally {
			zmqSocket.close();
		}
	}
}
