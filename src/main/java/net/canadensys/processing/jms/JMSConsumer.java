package net.canadensys.processing.jms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import net.canadensys.processing.message.ProcessingMessageIF;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.log4j.BasicConfigurator;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Java Messaging System message consumer.
 * 
 * @author canadensys
 *
 */
public class JMSConsumer{
	
	public String brokerURL;
	private boolean isOpen = false;

	// Name of the queue we will receive messages from
	private static String QUEUE_NAME = "Importer.Queue";
	private static String CONTROL_TOPIC = "Importer.Topic.Control";
	
	private Connection connection;
	private MessageConsumer consumer;
	
	//Topic connection is used to public control commands
	private TopicConnection topicConnection;
	private TopicSubscriber subscriber;
	
	private List<JMSConsumerMessageHandler> registeredHandlers;
	
	//Jackson Mapper to map JSON into Java object
	private ObjectMapper om;
	
	public JMSConsumer(String brokerURL){
		this.brokerURL = brokerURL;
		registeredHandlers = new ArrayList<JMSConsumerMessageHandler>();
	}
	
	public void setBrokerURL(String brokerURL){
		if(isOpen){
			throw new IllegalStateException("Can not set broker URL if the connection is started.");
		}
		this.brokerURL = brokerURL;
	}
	
	public String getBrokerUrl(){
		return brokerURL;
	}
	
	/**
	 * Register a handler to notify when we receive a message
	 * @param handler
	 */
	public void registerHandler(JMSConsumerMessageHandler handler){
		registeredHandlers.add(handler);
	}
	
	public void open() {
		om = new ObjectMapper();
		BasicConfigurator.configure();
		// Getting JMS connection from the server
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);
		
		try{
			connection = connectionFactory.createConnection();
			connection.start();
	
			// Creating session for sending messages
			Session session = connection.createSession(false,
					Session.AUTO_ACKNOWLEDGE);
	
			JMSMessageListener msgListener = new JMSMessageListener();
			
			// Getting the queue
			Queue queue = session.createQueue(QUEUE_NAME);
	
			// MessageConsumer is used for receiving (consuming) messages
			consumer = session.createConsumer(queue);
			consumer.setMessageListener(msgListener);
			
			//Control message
			topicConnection = connectionFactory.createTopicConnection();
			topicConnection.start();
			TopicSession subSession = topicConnection.createTopicSession(false,Session.AUTO_ACKNOWLEDGE);
			subscriber = subSession.createSubscriber(subSession.createTopic(CONTROL_TOPIC));
			subscriber.setMessageListener(msgListener);
		}
		catch(JMSException jmsEx){
			jmsEx.printStackTrace();
		}
	}

	public void close() {
		try {
			connection.close();
			topicConnection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	private class JMSMessageListener implements MessageListener{
		@Override
		public void onMessage(Message message) {
			// There are many types of Message and TextMessage
			// is just one of them. Producer sent us a TextMessage
			// so we must cast to it to get access to its .getText()
			// method.
			if (message instanceof TextMessage) {
				TextMessage msg = (TextMessage) message;
				
				try {
					Class<?> msgClass = Class.forName(ObjectUtils.defaultIfNull(msg.getStringProperty("MessageClass"), Object.class.getCanonicalName()));
					//validate if we can instantiate
					for(JMSConsumerMessageHandler currMsgHandler : registeredHandlers){
						if(currMsgHandler.getMessageClass().equals(msgClass)){
							ProcessingMessageIF chunk = (ProcessingMessageIF)om.readValue(msg.getText(), msgClass);
							currMsgHandler.handleMessage(chunk);
							break;
						}
						
						//TODO : add support for ControlMessageIF
						//TODO : raise error if no handler can process it
					}
				} catch (JMSException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (JsonParseException e) {
					e.printStackTrace();
				} catch (JsonMappingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
