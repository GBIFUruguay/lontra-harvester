package net.canadensys.harvester.jms;

import net.canadensys.harvester.message.ProcessingMessageIF;

/**
 * Interface to register to JMSConsumer as a message handler
 * @author canadensys
 *
 */
public interface JMSConsumerMessageHandler {
	
	/**
	 * The the class of the message the implementation can handle
	 * @return
	 */
	public Class<?> getMessageClass();
	
	/**
	 * Handle a message of the type given by getMessageClass() function.
	 * @param message
	 */
	public void handleMessage(ProcessingMessageIF message);

}
