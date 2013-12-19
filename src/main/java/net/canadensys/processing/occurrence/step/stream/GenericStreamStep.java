package net.canadensys.processing.occurrence.step.stream;

import java.util.Calendar;
import java.util.List;
import java.util.Map;

import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.ItemWriterIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.jms.JMSConsumerMessageHandler;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.message.DefaultMessage;

/**
 * Generic step to stream user defined object (defined by T).
 * 
 * @author canadensys
 *
 * @param <T>
 */
public class GenericStreamStep<T> implements ProcessingStepIF{

	private ItemReaderIF<T> reader;
	private ItemWriterIF<DefaultMessage> writer;
	private ItemProcessorIF<T, T> lineProcessor;

	private int numberOfRecords = 0;
	private Map<SharedParameterEnum,Object> sharedParameters;
	
	//List of message handlers that will received the streamed messages
	private List<Class< ? extends JMSConsumerMessageHandler>> targetedMsgHandlerList;

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
		for(Class<?> currClass : targetedMsgHandlerList){
			DefaultMessage dmsg = new DefaultMessage();
			dmsg.setTimestamp(Calendar.getInstance().getTime().toString());
			dmsg.setMsgHandlerClass(currClass);
			dmsg.setContent(obj);
			dmsg.setContentClass(obj.getClass());
			writer.write(dmsg);
		}
	}
	
	public int getNumberOfRecords(){
		return numberOfRecords;
	}
	
	public void setReader(ItemReaderIF<T> reader) {
		this.reader = reader;
	}
	public void setWriter(ItemWriterIF<DefaultMessage> writer) {
		this.writer = writer;
	}
	public void setDwcaLineProcessor(
			ItemProcessorIF<T, T> lineProcessor) {
		this.lineProcessor = lineProcessor;
	}
	
	public List<Class<? extends JMSConsumerMessageHandler>> targetedStepList() {
		return targetedMsgHandlerList;
	}
	public void setMessageClasses(
			List<Class<? extends JMSConsumerMessageHandler>> targetedMsgHandlerList) {
		this.targetedMsgHandlerList = targetedMsgHandlerList;
	}
}
