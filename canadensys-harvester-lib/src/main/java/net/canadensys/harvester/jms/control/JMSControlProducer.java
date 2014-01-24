package net.canadensys.harvester.jms.control;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import net.canadensys.harvester.message.ControlMessageIF;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JMS control message publisher.
 * @author canadensys
 *
 */
public class JMSControlProducer {
	private static final Logger LOGGER = Logger.getLogger(JMSControlProducer.class);
	public static String CONTROL_TOPIC = "Harvester.Topic.Control";

	private final String brokerURL;
	
	// Topic connection is used to public control commands
	private TopicConnection topicConnection;
	private TopicPublisher publisher;
	private TopicSession topicSession;
	
	// Jackson Mapper to write Java object into JSON
	private ObjectMapper om;
	
	public JMSControlProducer(String brokerURL){
		this.brokerURL = brokerURL;
	}
	
	public void open() {
		om = new ObjectMapper();
		//do not serialize null data
		om.setSerializationInclusion(Include.NON_NULL);
		
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);

		try {
			// Topic
			topicConnection = factory.createTopicConnection();
			topicConnection.start();
			topicSession = topicConnection.createTopicSession(
					false, Session.AUTO_ACKNOWLEDGE);
			publisher = topicSession.createPublisher(topicSession
					.createTopic(CONTROL_TOPIC));
		} catch (JMSException jEx) {
			LOGGER.fatal("Can not initialize JMSErrorReporter", jEx);
		}
	}
	
	public void close() {
		try {
			topicSession.close();
			topicConnection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Publish a message to the broker. Published message will be read by all
	 * consumer of the topic.
	 * 
	 * @param control
	 */
	public void publish(ControlMessageIF controlMsg) {
		TextMessage message;
		try {
			controlMsg.setNodeIdentifier(topicConnection.getClientID());
			message = topicSession.createTextMessage(om.writeValueAsString(controlMsg));
			message.setStringProperty("MessageClass", controlMsg.getClass().getCanonicalName());
			publisher.publish(message);
		} catch (JMSException e) {
			LOGGER.fatal("Can not publish control message", e);
		} catch (JsonProcessingException e) {
			LOGGER.fatal("Can not publish control message", e);
		}
	}
}
