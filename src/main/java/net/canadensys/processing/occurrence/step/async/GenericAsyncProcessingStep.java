package net.canadensys.processing.occurrence.step.async;

import java.util.Map;

import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.ItemWriterIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.jms.JMSConsumerMessageHandler;
import net.canadensys.processing.message.ProcessingMessageIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.message.DefaultMessage;

/**
 * Generic asynchronous step that handles user defined object (defined by T) received through DefaultMessage.
 * The user defined object is then processed and written to the defined writer.
 * @author canadensys
 *
 * @param <T> type of object received in the message and then sent to the processor
 * @param <S> type of object out of the processor that will be written
 */
public class GenericAsyncProcessingStep<T,S> implements ProcessingStepIF,JMSConsumerMessageHandler{
	
	private ItemProcessorIF<T, S> itemProcessor;
	private ItemWriterIF<S> writer;
	private Class<T> messageContentClass;
	
	/**
	 * 
	 * @param classOfT class object of T to allow explicit cast
	 */
	public GenericAsyncProcessingStep(Class<T> classOfT){
		messageContentClass = classOfT;
	}
	
	@Override
	public Class<?> getMessageClass() {
		return DefaultMessage.class;
	}

	@Override
	public void handleMessage(ProcessingMessageIF message) {
		long t = System.currentTimeMillis();
		Object obj = ((DefaultMessage)message).getContent();
		T data = messageContentClass.cast(obj);
		S processedData = itemProcessor.process(data,null);
		
		writer.write(processedData);
		System.out.println("Reading msg + Writing raw :" + ( System.currentTimeMillis()-t) + "ms");
	}

	@Override
	public void preStep(Map<SharedParameterEnum, Object> sharedParameters)
			throws IllegalStateException {
		if(writer == null){
			throw new IllegalStateException("No writer defined");
		}
		if(itemProcessor == null){
			throw new IllegalStateException("No processor defined");
		}
		writer.openWriter();
		itemProcessor.init();
	}

	@Override
	public void postStep() {
		writer.closeWriter();
		itemProcessor.destroy();
	}

	/**
	 * No implemented, async step
	 */
	@Override
	public void doStep() {}

	public ItemProcessorIF<T, S> getItemProcessor() {
		return itemProcessor;
	}
	public void setItemProcessor(ItemProcessorIF<T, S> itemProcessor) {
		this.itemProcessor = itemProcessor;
	}

	public ItemWriterIF<S> getWriter() {
		return writer;
	}
	public void setWriter(ItemWriterIF<S> writer) {
		this.writer = writer;
	};
}
