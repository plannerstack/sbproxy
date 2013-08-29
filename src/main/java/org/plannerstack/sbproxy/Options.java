package org.plannerstack.sbproxy;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Command line options and properties.
 */
class Options {
	private static final String PROPERTY_DEFAULTS = "/sbproxy-defaults.properties";
	private static final String DEFAULT_PROPERTIES_FILE = "sbproxy.properties";

	private CmdLineParser parser;

	@Option(name = "-c", metaVar = "FILE",
			usage = "path to the configuration file (default: " + DEFAULT_PROPERTIES_FILE + ")")
	private String propertiesFile = DEFAULT_PROPERTIES_FILE;

	String azureNamespace;
	String azureTopic;
	String azureSubscription;
	String azureAuthname;
	String azurePassword;
	int maxMsgSize;
	String zmqAddress;
	boolean zmqCompression;

	public Options() {
		parser = new CmdLineParser(this);
	}

	public void printUsage(PrintStream stream) {
		System.err.print("java -jar sbproxy.jar");
		parser.printSingleLineUsage(stream);
		System.err.println();
		parser.printUsage(stream);
	}

	public void parse(String[] args) throws CmdLineException {
		parser.parseArgument(args);
		try {
			loadProperties();
		} catch (IOException | NullPointerException | NumberFormatException e) {
			throw new CmdLineException(parser, e);
		}
	}

	private void loadProperties() throws IOException, NumberFormatException {
		try (
			InputStream defaultsStream = CmdLine.class.getResourceAsStream(PROPERTY_DEFAULTS);
			InputStream propStream = new FileInputStream(propertiesFile);
		) {
			Properties properties = new Properties();
			properties.load(defaultsStream);
			properties.load(propStream);
			azureNamespace = getProperty(properties, "azure.namespace");
			azureTopic = getProperty(properties, "azure.servicebus.topic");
			azureSubscription = getProperty(properties, "azure.servicebus.subscription");
			azureAuthname = getProperty(properties, "azure.authentication.name");
			azurePassword = getProperty(properties, "azure.authentication.password");
			maxMsgSize = Integer.parseInt(getProperty(properties, "max_message_size"));
			zmqAddress = getProperty(properties, "zeromq.address");
			zmqCompression = Boolean.parseBoolean(getProperty(properties, "zeromq.compress_messages"));
		}
	}

	private String getProperty(Properties properties, String key) {
		return checkNotNull(properties.getProperty(key), "property not set: %s", key);
	}
}
