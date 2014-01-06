package net.canadensys.harvester.occurrence.step;

import java.util.Map;

import net.canadensys.dataportal.occurrence.model.ResourceContactModel;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.jms.JMSConsumerMessageHandler;
import net.canadensys.harvester.message.ProcessingMessageIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.message.SaveResourceContactMessage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Step taking a SaveResourceContactMessage from JMS and writing a ResourceContactModel to a writer
 * NOT thread safe
 * @author canadensys
 *
 */
public class InsertResourceContactStep implements ProcessingStepIF,JMSConsumerMessageHandler{

	@Autowired
	@Qualifier("resourceContactWriter")
	private ItemWriterIF<ResourceContactModel> writer;

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
		return SaveResourceContactMessage.class;
	}

	@Override
	public void handleMessage(ProcessingMessageIF message) {
		long t = System.currentTimeMillis();
		ResourceContactModel rcm = ((SaveResourceContactMessage)message).getResourceContactModel();
		writer.write(rcm);
		System.out.println("Reading msg + Writing Resource Contact :" + ( System.currentTimeMillis()-t) + "ms");
	}
	
	/**
	 * No implemented, async step
	 */
	@Override
	public void doStep() {};
	
	public void setWriter(ItemWriterIF<ResourceContactModel> writer){
		this.writer = writer;
	}

}
