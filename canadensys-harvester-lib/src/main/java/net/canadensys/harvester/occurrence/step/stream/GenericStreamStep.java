package net.canadensys.harvester.occurrence.step.stream;

import java.util.Calendar;
import java.util.List;
import java.util.Map;

import net.canadensys.harvester.ItemProcessorIF;
import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.exception.WriterException;
import net.canadensys.harvester.jms.JMSConsumerMessageHandlerIF;
import net.canadensys.harvester.message.ProcessingMessageIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.message.DefaultMessage;

import org.apache.log4j.Logger;

/**
 * Generic step to stream user defined object (defined by T).
 * 
 * @author canadensys
 *
 * @param <T>
 */
public class GenericStreamStep<T> implements ProcessingStepIF{

	private static final Logger LOGGER = Logger.getLogger(GenericStreamStep.class);
	
	private ItemReaderIF<T> reader;
	private ItemWriterIF<ProcessingMessageIF> writer;
	private ItemProcessorIF<T, T> lineProcessor;

	private int numberOfRecords = 0;
	private Map<SharedParameterEnum,Object> sharedParameters;
	
	//List of message handlers that will received the streamed messages
	private List<Class< ? extends JMSConsumerMessageHandlerIF>> targetedMsgHandlerList;

	@Override
	public void preStep(Map<SharedParameterEnum,Object> sharedParameters) throws IllegalStateException {
		if(writer == null){
			throw new IllegalStateException("No writer defined");
		}
		if(reader == null){
			throw new IllegalStateException("No reader defined");
		}
		if( targetedMsgHandlerList == null || targetedMsgHandlerList.size() <= 0){
			throw new IllegalStateException("No targeted message handler");
		}
		
		this.sharedParameters = sharedParameters;
		reader.openReader(sharedParameters);
		writer.openWriter();
		
		if(lineProcessor != null){
			lineProcessor.init();
		}
	}

	@Override
	public void postStep() {
		writer.closeWriter();
		reader.closeReader();
		
		if(lineProcessor != null){
			lineProcessor.destroy();
		}
	}

	@Override
	public void doStep() {
		long t= System.currentTimeMillis();
		T currObject = reader.read();
		while(currObject != null){
			if(lineProcessor != null){
				currObject = lineProcessor.process(currObject, sharedParameters);
			}

			writeObject(currObject);
			
			currObject = reader.read();
			numberOfRecords++;
		}
		System.out.println("Streaming the file took :" + (System.currentTimeMillis()-t) + " ms");
		sharedParameters.put(SharedParameterEnum.NUMBER_OF_RECORDS,numberOfRecords);
	}
	
	/**
	 * Create one message per defined message class
	 * @param obj
	 */
	private void writeObject(Object obj){
		try{
			for(Class<?> currClass : targetedMsgHandlerList){
				DefaultMessage dmsg = new DefaultMessage();
				dmsg.setTimestamp(Calendar.getInstance().getTime().toString());
				dmsg.setMsgHandlerClass(currClass);
				dmsg.setContent(obj);
				dmsg.setContentClass(obj.getClass());
				writer.write(dmsg);
			}
		}
		catch(WriterException e){
			LOGGER.fatal(e);
		}
	}
	
	public int getNumberOfRecords(){
		return numberOfRecords;
	}
	
	public void setReader(ItemReaderIF<T> reader) {
		this.reader = reader;
	}
	public void setWriter(ItemWriterIF<ProcessingMessageIF> writer) {
		this.writer = writer;
	}
	public void setDwcaLineProcessor(
			ItemProcessorIF<T, T> lineProcessor) {
		this.lineProcessor = lineProcessor;
	}
	
	public List<Class<? extends JMSConsumerMessageHandlerIF>> targetedStepList() {
		return targetedMsgHandlerList;
	}
	public void setMessageClasses(
			List<Class<? extends JMSConsumerMessageHandlerIF>> targetedMsgHandlerList) {
		this.targetedMsgHandlerList = targetedMsgHandlerList;
	}
}
