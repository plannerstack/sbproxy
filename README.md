Azure Service Bus to ZeroMQ proxy
=================================

[![Build Status](https://travis-ci.org/plannerstack/sbproxy.png)](https://travis-ci.org/plannerstack/sbproxy)

This tool receives messages from a Azure Service Bus topic subscription and forwards them to a ZeroMQ socket.


Building
--------

The project uses Maven for its life-cycle management. Build the JAR file with the following command:

	mvn package
	
	
Running the proxy
-----------------

In the Maven *package* phase an artifact named `sbproxy-VERSION-shaded.jar` will be created that contains all dependencies and can be used as a stand-alone command line tool:

    java -jar target/sbproxy-VERSION-shaded.jar
    
The proxy is configured using a properties file named `sbproxy.properties` in the current directory. You can change the location of the properties file using the `-c` command line option:

    java -jar target/sbproxy-VERSION-shaded.jar -c /etc/sbproxy.properties

A skeleton properties file is distributed with the project source in `src/main/resources/sbproxy.properties-distrib`.


Installing as a service
-----------------------

An Upstart script template is provided in `src/main/resources/upstart.conf`. In the Maven *process-resources* phase this file is copied and variable-expanded in `target/classes/upstart.conf`. Install the shaded JAR file in `/usr/local/share/java`, configure the proxy in `/etc/sbproxy.properties` and copy the Upstart script to `/etc/init/sbproxy.conf`. Then run the following command to start the `sbproxy` service:

    start sbproxy
    
You can find the logs at `/var/log/upstart/sbproxy.log`. They should be rotated automatically.