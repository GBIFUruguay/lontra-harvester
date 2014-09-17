package net.canadensys.harvester.occurrence.step;

import java.util.Map;

import net.canadensys.dataportal.occurrence.model.ResourceInformationModel;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.exception.WriterException;
import net.canadensys.harvester.jms.JMSConsumerMessageHandlerIF;
import net.canadensys.harvester.jms.control.JMSControlProducer;
import net.canadensys.harvester.message.ProcessingMessageIF;
import net.canadensys.harvester.message.control.NodeErrorControlMessage;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.message.SaveResourceInformationMessage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Step taking a SaveResourceInformationMessage from JMS and writing a ResourceInformationModel to a writer
 * NOT thread safe
 * @author canadensys
 *
 */
public class InsertResourceInformationStep implements ProcessingStepIF,JMSConsumerMessageHandlerIF{

	@Autowired
	@Qualifier("resourceInformationWriter")
	private ItemWriterIF<ResourceInformationModel> writer;
	
	@Autowired
	private JMSControlProducer errorReporter;

	@Override
	public void preStep(Map<SharedParameterEnum,Object> sharedParameters) throws IllegalStateException{
		if(writer == null){
			throw new IllegalStateException("No writer defined");
		}
		if(errorReporter == null){
			throw new IllegalStateException("No errorReporter defined");
		}
		writer.openWriter();
		errorReporter.open();
	}

	@Override
	public void postStep() {
		writer.closeWriter();
		errorReporter.close();
	}
	
	@Override
	public Class<?> getMessageClass() {
		return SaveResourceInformationMessage.class;
	}

	@Override
	public boolean handleMessage(ProcessingMessageIF message) {
		long t = System.currentTimeMillis();
		ResourceInformationModel rcm = ((SaveResourceInformationMessage)message).getResourceInformationModel();
		try {
			writer.write(rcm);
		} catch (WriterException e) {
			errorReporter.publish(new NodeErrorControlMessage(e));
			return false;
		}
		System.out.println("Reading msg + Writing Resource Information :" + ( System.currentTimeMillis()-t) + "ms");
		return true;
	}
	
	/**
	 * No implemented, async step
	 */
	@Override
	public void doStep() {};
	
	public void setWriter(ItemWriterIF<ResourceInformationModel> writer){
		this.writer = writer;
	}
	
	@Override
	public String getTitle() {
		return "Inserting resource Information data";
	}
}
