package net.canadensys.harvester.jms;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.web.RemoteJMXBrokerFacade;
import org.apache.activemq.web.config.SystemPropertiesConfiguration;

/**
 * Manager for JMS Broker using JMX.
 * 
 * JMX must be enable on the ActiveMQ broker.
 * 
 * --activemq.xml--
 * 
 * <broker ... useJmx="true">
 * 
 * <managementContext> <managementContext connectorPort="2011"/>
 * </managementContext>
 * 
 * --startup script--
 * 
 * ACTIVEMQ_SUNJMX_START=
 * "$ACTIVEMQ_SUNJMX_START -Dcom.sun.management.jmxremote.ssl=false"
 * ACTIVEMQ_SUNJMX_START=
 * "$ACTIVEMQ_SUNJMX_START -Dcom.sun.management.jmxremote.password.file=${ACTIVEMQ_CONF}/jmx.password"
 * ACTIVEMQ_SUNJMX_START=
 * "$ACTIVEMQ_SUNJMX_START -Dcom.sun.management.jmxremote.access.file=${ACTIVEMQ_CONF}/jmx.access"
 * 
 * @author cgendreau
 * 
 */
public class JMSManager {

	private final RemoteJMXBrokerFacade facade;

	/**
	 * 
	 * @param jmxHost
	 * @param jmxPort
	 * @param jmxUser
	 *            as defined in jmx.access on the JMS broker
	 * @param jmxPassword
	 *            as defined in jmx.password on the JMS broker
	 */
	public JMSManager(String jmxHost, int jmxPort, String jmxUser,
			String jmxPassword) {
		this.facade = new RemoteJMXBrokerFacade();

		System.setProperty("webconsole.jmx.url",
				"service:jmx:rmi:///jndi/rmi://" + jmxHost + ":" + jmxPort
						+ "/jmxrmi");
		System.setProperty("webconsole.jmx.user", jmxUser);
		System.setProperty("webconsole.jmx.password", jmxPassword);

		SystemPropertiesConfiguration w = new SystemPropertiesConfiguration();
		facade.setConfiguration(w);
	}

	/**
	 * Purge the queue.
	 * 
	 * @param queueName
	 */
	public void cleanUp(String queueName) {
		try {
			QueueViewMBean queue = facade.getQueue(queueName);
			if (queue != null) {
				queue.purge();
				long size = queue.getQueueSize();
				if (size > 0)
					throw new IllegalStateException("Queue " + queueName
							+ " could not be purged.");
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Get the number of element in the queue.
	 * 
	 * @param queueName
	 * @return
	 */
	public long getQueueSize(String queueName) {
		try {
			QueueViewMBean queue = facade.getQueue(queueName);
			if (queue != null) {
				return queue.getQueueSize();
			}
			// should we throw an exception?
			return 0;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	public static void main(String[] args) {
		JMSManager manager = new JMSManager("localhost", 2011, "admin",
				"activemq");
		manager.cleanUp("toto");
	}

}
