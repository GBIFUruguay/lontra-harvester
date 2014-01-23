package net.canadensys.harvester.jms.control;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import net.canadensys.harvester.message.ControlMessageIF;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JMS control message consumer.
 * @author canadensys
 *
 */
public class JMSControlConsumer {
	private static final Logger LOGGER = Logger.getLogger(JMSControlConsumer.class);
	
	public String brokerURL;
		
	//Topic connection is used to public control commands
	private TopicConnection topicConnection;
	private TopicSubscriber subscriber;
	private TopicSession subSession;
	
	//Jackson Mapper to map JSON into Java object
	private ObjectMapper om;
	
	private List<JMSControlConsumerMessageHandlerIF> registeredHandlers;
	
	public JMSControlConsumer(String brokerURL){
		this.brokerURL = brokerURL;
		registeredHandlers = new ArrayList<JMSControlConsumerMessageHandlerIF>();
	}
	
	public void open() {
		om = new ObjectMapper();
		BasicConfigurator.configure();
		// Getting JMS connection from the server
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);
		
		
		try{				
			//Control message
			topicConnection = connectionFactory.createTopicConnection();
			topicConnection.start();
			subSession = topicConnection.createTopicSession(false,Session.AUTO_ACKNOWLEDGE);
			subscriber = subSession.createSubscriber(subSession.createTopic(JMSControlProducer.CONTROL_TOPIC));
			subscriber.setMessageListener(new JMSControlMessageListener());
		}
		catch(JMSException jmsEx){
			LOGGER.fatal("Can not initialize JMSConsumer", jmsEx);
		}
	}
	
	public void close() {
		try {
			subSession.close();
			topicConnection.close();	
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Register a handler to notify when we receive a message
	 * @param handler
	 */
	public void registerHandler(JMSControlConsumerMessageHandlerIF handler){
		registeredHandlers.add(handler);
	}
	
	private class JMSControlMessageListener implements MessageListener{
		@Override
		public void onMessage(Message message) {
			// Producer sent us a TextMessage
			// so we must cast to it to get access to its .getText()
			// method.
			if (message instanceof TextMessage) {
				TextMessage msg = (TextMessage) message;
				try {
					Class<?> msgClass = Class.forName(ObjectUtils.defaultIfNull(msg.getStringProperty("MessageClass"), Object.class.getCanonicalName()));
					//validate if we can instantiate
					for(JMSControlConsumerMessageHandlerIF currMsgHandler : registeredHandlers){
						
						if(currMsgHandler.getMessageClass().equals(msgClass)){
							ControlMessageIF controlMessage = (ControlMessageIF)om.readValue(msg.getText(), msgClass);
							if(!currMsgHandler.handleMessage(controlMessage)){
								throw new RuntimeException("Error while handling the control message");
							}
							break;
						}
						
						//TODO : add support for ControlMessageIF
						//TODO : raise error if no handler can process it
					}
				} catch (JMSException e) {
					LOGGER.fatal("Can not consume message ", e);
				} catch (ClassNotFoundException e) {
					LOGGER.fatal("Can not consume message ", e);
				} catch (JsonParseException e) {
					LOGGER.fatal("Can not consume message ", e);
				} catch (JsonMappingException e) {
					LOGGER.fatal("Can not consume message ", e);
				} catch (IOException e) {
					LOGGER.fatal("Can not consume message ", e);
				}
			}
		}
	}

}
