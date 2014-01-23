package net.canadensys.harvester.jms.control;

import net.canadensys.harvester.message.ControlMessageIF;

/**
 * Message handler for control messages.
 * @author canadensys
 *
 */
public interface JMSControlConsumerMessageHandlerIF {
	/**
	 * The the class of the message the implementation can handle
	 * @return
	 */
	public Class<?> getMessageClass();
	
	/**
	 * Handle a message of the type given by getMessageClass() function.
	 * @param message
	 * @return the message was handled successfully or not
	 */
	public boolean handleMessage(ControlMessageIF message);
}
