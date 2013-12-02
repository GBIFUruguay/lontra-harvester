package net.canadensys.processing.occurrence.step;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import net.canadensys.dataportal.occurrence.model.OccurrenceModel;
import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.ItemWriterIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.jms.JMSConsumerMessageHandler;
import net.canadensys.processing.message.ProcessingMessageIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.message.ProcessOccurrenceMessage;

/**
 * Step taking a ProcessOccurrenceMessage from JMS message, process a Occurrence Raw object list, writing the result.
 * NOT thread safe
 * @author canadensys
 *
 */
public class ProcessInsertOccurrenceStep implements ProcessingStepIF,JMSConsumerMessageHandler{

	@Autowired
	@Qualifier("occurrenceProcessor")
	private ItemProcessorIF<OccurrenceRawModel, OccurrenceModel> processor;
	
	@Autowired
	@Qualifier("occurrenceWriter")
	private ItemWriterIF<OccurrenceModel> writer;
	
	@Override
	public void preStep(Map<SharedParameterEnum,Object> sharedParameters) throws IllegalStateException {
		if(writer == null){
			throw new IllegalStateException("No writer defined");
		}
		if(processor == null){
			throw new IllegalStateException("No processor defined");
		}
		writer.openWriter();
	}

	@Override
	public void postStep() {
		writer.closeWriter();
	}
	
	/**
	 * No implemented, async step
	 */
	@Override
	public void doStep() {};
	
	@Override
	public Class<?> getMessageClass() {
		return ProcessOccurrenceMessage.class;
	}

	@Override
	public void handleMessage(ProcessingMessageIF message) {
		List<OccurrenceRawModel> occRawList = ((ProcessOccurrenceMessage)message).getRawModelList();
		List<OccurrenceModel> occList = new ArrayList<OccurrenceModel>();
		
		for(OccurrenceRawModel currRawModel : occRawList){
			occList.add(processor.process(currRawModel, null));
		}
		writer.write(occList);
	}
	
	public void setProcessor(ItemProcessorIF<OccurrenceRawModel, OccurrenceModel> processor){
		this.processor = processor;
	}
	
	public void setWriter(ItemWriterIF<OccurrenceModel> writer){
		this.writer = writer;
	}
}
