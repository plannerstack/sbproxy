Azure Service Bus to ZeroMQ proxy
=================================

Build the JAR file using Maven:

	mvn package

The proxy is configured using a properties file. The default location is
/etc/sbproxy.properties. A skeleton properties file is distributed with the
project source: src/main/resources/sbproxy.properties-distrib

Start the proxy:

	java -jar target/sbproxy-VERSION.jar -c PROPERTY_FILE
