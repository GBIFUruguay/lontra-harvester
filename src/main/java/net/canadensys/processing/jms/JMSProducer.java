package net.canadensys.processing.jms;

import java.io.IOException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import net.canadensys.processing.message.ControlMessageIF;
import net.canadensys.processing.message.ProcessingMessageIF;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Java Messaging System message producer
 * @author canadensys
 *
 */
public class JMSProducer{

	private String brokerURL;
	
	// Name of the queue we will sent messages into
	private static String QUEUE_NAME = "Importer.Queue";
	private static String CONTROL_TOPIC = "Importer.Topic.Control";
	
	private Connection connection;
	private Session session;
	private MessageProducer producer;
	
	//Topic connection is used to public control commands
	private TopicConnection topicConnection;
	private TopicPublisher publisher;
	
	//Jackson Mapper to write Java object into JSON
	private ObjectMapper om;
	
	public JMSProducer(String brokerURL){
		this.brokerURL = brokerURL;
	}

	public void init() {
		om = new ObjectMapper();
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
		
		try{
			// Getting JMS connection from the server and starting it
			connection = factory.createConnection();
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	
			Destination destination = session.createQueue(QUEUE_NAME);
			producer = session.createProducer(destination);
			
			//Topic
			topicConnection = factory.createTopicConnection();
			topicConnection.start();
			TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
			publisher = topicSession.createPublisher(topicSession.createTopic(CONTROL_TOPIC));
		}
		catch(JMSException jEx){
			jEx.printStackTrace();
		}
	}

	public void close() {
		try {
			connection.stop();
			topicConnection.stop();
			connection.close();
			topicConnection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Send message to the broker.
	 * @param element
	 */
	public void send(ProcessingMessageIF element) {
		TextMessage message;
		try {
			message = session.createTextMessage(om.writeValueAsString(element));
			message.setStringProperty("MessageClass", element.getClass().getCanonicalName());
			producer.send(message);
		} catch (JMSException e) {
			e.printStackTrace();
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Publish a message to the broker.
	 * Published message will be read by all consumer of the topic.
	 * @param control
	 */
	public void publish(ControlMessageIF controlMsg){
		TextMessage message;
		try {
			message = session.createTextMessage(om.writeValueAsString(controlMsg));
			publisher.publish(message);
		} catch (JMSException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
}
