package net.canadensys.harvester.jms;

import java.io.IOException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import net.canadensys.harvester.message.ProcessingMessageIF;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Java Messaging System message producer.
 * 
 * @author canadensys
 * 
 */
public class JMSProducer {

	private static final Logger LOGGER = Logger.getLogger(JMSProducer.class);

	private String brokerURL;
	private boolean isOpen = false;

	// Name of the queue we will sent messages into
	public static String QUEUE_NAME = "Harvester.Queue";

	private Connection connection;
	private Session session;
	private MessageProducer producer;

	// Jackson Mapper to write Java object into JSON
	private ObjectMapper om;

	public JMSProducer(String brokerURL) {
		this.brokerURL = brokerURL;
	}
	
	public void setBrokerURL(String brokerURL){
		if(isOpen){
			throw new IllegalStateException("Can not set broker URL if the connection is started.");
		}
		this.brokerURL = brokerURL;
	}

	public void close() {
		try {
			connection.stop();
			connection.close();
			isOpen = false;
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	public void init() {
		om = new ObjectMapper();
		//do not serialize null data
		om.setSerializationInclusion(Include.NON_NULL);
		
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
				brokerURL);
		
		try {
			// Getting JMS connection from the server and starting it
			connection = factory.createConnection();
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			Destination destination = session.createQueue(QUEUE_NAME);
			isOpen = true;
			producer = session.createProducer(destination);
		} catch (JMSException jEx) {
			LOGGER.fatal("Can not initialize JMSProducer", jEx);
		}
	}

	/**
	 * Send message to the broker.
	 * 
	 * @param element
	 */
	public void send(ProcessingMessageIF element) {
		TextMessage message;
		try {
			message = session.createTextMessage(om.writeValueAsString(element));
			message.setStringProperty("MessageClass", element.getClass().getCanonicalName());
			producer.send(message);
		} catch (JMSException e) {
			LOGGER.fatal("Can not send message", e);
		} catch (JsonGenerationException e) {
			LOGGER.fatal("Can not send message", e);
		} catch (JsonMappingException e) {
			LOGGER.fatal("Can not send message", e);
		} catch (IOException e) {
			LOGGER.fatal("Can not send message", e);
		}
	}
}
