package net.canadensys.harvester.jms;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.web.RemoteJMXBrokerFacade;
import org.apache.activemq.web.config.SystemPropertiesConfiguration;

public class JMSManager {

	private final RemoteJMXBrokerFacade facade;

	public JMSManager() {
		this.facade = new RemoteJMXBrokerFacade();

		// System.setProperty("webconsole.jms.url", "tcp://localhost:61616");
		System.setProperty("webconsole.jmx.url",
				"service:jmx:rmi:///jndi/rmi://localhost:2011/jmxrmi");
		System.setProperty("webconsole.jmx.user", "admin");
		System.setProperty("webconsole.jmx.password", "activemq");

		SystemPropertiesConfiguration w = new SystemPropertiesConfiguration();
		System.out.println(w.getJmxUrls().toArray()[0] + "," + w.getJmxUser()
				+ "," + w.getJmxPassword());
		facade.setConfiguration(w);
		try {
			System.out.println(facade.getBrokerAdmin());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void cleanUp(String queueName) {
		try {
			System.out.println(facade.getBrokerName());
			QueueViewMBean queue = facade.getQueue(queueName);
			if (queue != null) {
				queue.purge();
				long size = queue.getQueueSize();
				if (size > 0)
					throw new IllegalStateException("Fila " + queueName
							+ " não foi limpa!");
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	public long getQueueSize(String queueName) {
		try {
			QueueViewMBean queue = facade.getQueue(queueName);
			return (queue != null ? queue.getQueueSize() : 0);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	public static void main(String[] args) {
		JMSManager manager = new JMSManager();
		manager.cleanUp("toto");
	}

}
