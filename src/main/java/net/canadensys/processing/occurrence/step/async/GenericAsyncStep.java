package net.canadensys.processing.occurrence.step.async;

import java.util.Map;

import net.canadensys.processing.ItemWriterIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.jms.JMSConsumerMessageHandler;
import net.canadensys.processing.message.ProcessingMessageIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.message.DefaultMessage;

/**
 * Generic asynchronous step that handles user defined object (defined by T) received though DefaultMessage.
 * The user defined object is then written to the defined writer.
 * @author canadensys
 *
 * @param <T>
 */
public class GenericAsyncStep<T> implements ProcessingStepIF,JMSConsumerMessageHandler{
	
	private ItemWriterIF<T> writer;
	private Class<T> messageContentClass;
	
	/**
	 * 
	 * @param classOfT class object of T to allow casting
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
