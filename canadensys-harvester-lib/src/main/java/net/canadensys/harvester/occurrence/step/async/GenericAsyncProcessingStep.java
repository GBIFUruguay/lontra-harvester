package net.canadensys.harvester.occurrence.step.async;

import java.util.Map;

import net.canadensys.harvester.ItemProcessorIF;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.exception.WriterException;
import net.canadensys.harvester.jms.JMSConsumerMessageHandlerIF;
import net.canadensys.harvester.message.ProcessingMessageIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.message.DefaultMessage;

/**
 * Generic asynchronous step that handles user defined object (defined by T) received through DefaultMessage.
 * The user defined object is then processed and written to the defined writer.
 * @author canadensys
 *
 * @param <T> type of object received in the message and then sent to the processor
 * @param <S> type of object out of the processor that will be written
 */
public class GenericAsyncProcessingStep<T,S> implements ProcessingStepIF,JMSConsumerMessageHandlerIF{
	
	private ItemProcessorIF<T, S> itemProcessor;
	private ItemWriterIF<S> writer;
	private Class<T> messageContentClass;
	
	private String stepTitle = "Processing and Writing data using GenericAsyncProcessingStep";
	
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
	public boolean handleMessage(ProcessingMessageIF message) {
		Object obj = ((DefaultMessage)message).getContent();
		T data = messageContentClass.cast(obj);
		S processedData = itemProcessor.process(data,null);
		
		try {
			writer.write(processedData);
		} catch (WriterException e) {
			return false;
		}
		return true;
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
	}
	
	public void setTitle(String stepTitle) {
		this.stepTitle=stepTitle;
	}
	@Override
	public String getTitle() {
		return stepTitle;
	}
}
