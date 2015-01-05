package net.canadensys.harvester.occurrence.step;

import java.util.Calendar;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.ResourceMetadataModel;
import net.canadensys.harvester.ItemProcessorIF;
import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.StepIF;
import net.canadensys.harvester.exception.WriterException;
import net.canadensys.harvester.message.ProcessingMessageIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.message.SaveResourceInformationMessage;

import org.gbif.metadata.eml.Eml;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Step reading an EML file from a DarwinCore archive, process it, writing the result as a ProcessingMessageIF.
 * NOT thread safe
 * 
 * @author canadensys
 * 
 */
public class StreamEmlContentStep implements StepIF {

	@Autowired
	@Qualifier("dwcaEmlReader")
	private ItemReaderIF<Eml> reader;

	@Autowired
	@Qualifier("jmsWriter")
	private ItemWriterIF<ProcessingMessageIF> writer;

	@Autowired
	@Qualifier("resourceInformationProcessor")
	private ItemProcessorIF<Eml, ResourceMetadataModel> resourceInformationProcessor;

	private Map<SharedParameterEnum, Object> sharedParameters;

	@Override
	public void preStep(Map<SharedParameterEnum, Object> sharedParameters) {
		if (writer == null) {
			throw new IllegalStateException("No writer defined");
		}
		if (resourceInformationProcessor == null) {
			throw new IllegalStateException("No processor defined");
		}
		if (reader == null) {
			throw new IllegalStateException("No reader defined");
		}
		this.sharedParameters = sharedParameters;
		reader.openReader(sharedParameters);
		writer.openWriter();
		resourceInformationProcessor.init();
	}

	@Override
	public void postStep() {
		writer.closeWriter();
		resourceInformationProcessor.destroy();
		reader.closeReader();
	}

	@Override
	public void doStep() {
		// For now, we only read and stream the resource information
		SaveResourceInformationMessage srcm = new SaveResourceInformationMessage();
		srcm.setWhen(Calendar.getInstance().getTime().toString());

		Eml emlModel = reader.read();
		ResourceMetadataModel resourceInformationModel = resourceInformationProcessor.process(emlModel, sharedParameters);

		srcm.setResourceInformationModel(resourceInformationModel);

		try {
			writer.write(srcm);
		}
		catch (WriterException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getTitle() {
		return "Streaming EML";
	}
}
