package net.canadensys.harvester.occurrence.step.async;

import java.util.Map;

import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.jms.JMSConsumerMessageHandler;
import net.canadensys.harvester.message.ProcessingMessageIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.message.DefaultMessage;

/**
 * Generic asynchronous step that handles user defined object (defined by T) received though DefaultMessage.
 * The user defined object is then written to the defined writer.
 * This class does NOT contain any ItemProcessorIF
 * @author canadensys
 *
 * @param <T>
 */
public class GenericAsyncStep<T> implements ProcessingStepIF,JMSConsumerMessageHandler{
	
	private ItemWriterIF<T> writer;
	private Class<T> messageContentClass;
	
	/**
	 * 
	 * @param classOfT class object of T to allow explicit cast
	 */
	public GenericAsyncStep(Class<T> classOfT){
		messageContentClass = classOfT;
	}

	@Override
	public void preStep(Map<SharedParameterEnum,Object> sharedParameters) throws IllegalStateException{
		if(writer == null){
			throw new IllegalStateException("No writer defined");
		}
		writer.openWriter();
	}

	@Override
	public void postStep() {
		writer.closeWriter();
	}
	
	@Override
	public Class<?> getMessageClass() {
		return DefaultMessage.class;
	}
	
	/**
	 * This will be used to route object to the right GenericAsyncStep in case more than one is registered.
	 * @return
	 */
	public Class<?> getMessageContentClass() {
		return messageContentClass;
	}

	@Override
	public void handleMessage(ProcessingMessageIF message) {
		long t = System.currentTimeMillis();
		Object obj = ((DefaultMessage)message).getContent();
		writer.write(messageContentClass.cast(obj));
		System.out.println("Reading msg + Writing raw :" + ( System.currentTimeMillis()-t) + "ms");
	}
	
	/**
	 * No implemented, async step
	 */
	@Override
	public void doStep() {};
	
	public void setWriter(ItemWriterIF<T> writer){
		this.writer = writer;
	}
}
