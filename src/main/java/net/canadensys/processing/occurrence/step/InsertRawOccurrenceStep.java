package net.canadensys.processing.occurrence.step;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.processing.ItemWriterIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.jms.JMSConsumerMessageHandler;
import net.canadensys.processing.message.ProcessingMessageIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.message.SaveRawOccurrenceMessage;

/**
 * Step taking a SaveRawOccurrenceMessage from JMS and writing a Occurrence Raw object list to a writer
 * NOT thread safe
 * @author canadensys
 *
 */
public class InsertRawOccurrenceStep implements ProcessingStepIF,JMSConsumerMessageHandler{
	
	@Autowired
	@Qualifier("rawOccurrenceWriter")
	private ItemWriterIF<OccurrenceRawModel> writer;

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
		return SaveRawOccurrenceMessage.class;
	}

	@Override
	public void handleMessage(ProcessingMessageIF message) {
		long t = System.currentTimeMillis();
		List<OccurrenceRawModel> occRawList = ((SaveRawOccurrenceMessage)message).getRawModelList();
		writer.write(occRawList);
		System.out.println("Reading msg + Writing raw :" + ( System.currentTimeMillis()-t) + "ms");
	}
	
	/**
	 * No implemented, async step
	 */
	@Override
	public void doStep() {};
	
	public void setWriter(ItemWriterIF<OccurrenceRawModel> writer){
		this.writer = writer;
	}
}
