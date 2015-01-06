package net.canadensys.harvester.occurrence.step;

import java.util.Map;

import net.canadensys.dataportal.occurrence.model.ResourceMetadataModel;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.exception.WriterException;
import net.canadensys.harvester.jms.JMSConsumerMessageHandlerIF;
import net.canadensys.harvester.jms.control.JMSControlProducer;
import net.canadensys.harvester.message.ProcessingMessageIF;
import net.canadensys.harvester.message.control.NodeErrorControlMessage;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.message.SaveResourceInformationMessage;
import net.canadensys.harvester.occurrence.step.async.AbstractReceiverStep;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Step taking a SaveResourceInformationMessage from JMS and writing a ResourceInformationModel to a writer
 * NOT thread safe
 * 
 * @author canadensys
 * 
 */
public class InsertResourceInformationStep extends AbstractReceiverStep implements JMSConsumerMessageHandlerIF {

	@Autowired
	@Qualifier("resourceInformationWriter")
	private ItemWriterIF<ResourceMetadataModel> writer;

	@Autowired
	private JMSControlProducer errorReporter;

	@Override
	public void preStep(Map<SharedParameterEnum, Object> sharedParameters) throws IllegalStateException {
		if (writer == null) {
			throw new IllegalStateException("No writer defined");
		}
		if (errorReporter == null) {
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
		ResourceMetadataModel rcm = ((SaveResourceInformationMessage) message).getResourceInformationModel();
		if (rcm != null) {
			try {
				writer.write(rcm);
				return true;
			}
			catch (WriterException e) {
				errorReporter.publish(new NodeErrorControlMessage(e));
			}
		}
		errorReporter.publish(new NodeErrorControlMessage(new Exception("InsertResourceInformationStep :: ResourceInformationModel is null")));
		return false;
	}

	public void setWriter(ItemWriterIF<ResourceMetadataModel> writer) {
		this.writer = writer;
	}

	@Override
	public String getTitle() {
		return "Inserting resource Information data";
	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub
	}
}
