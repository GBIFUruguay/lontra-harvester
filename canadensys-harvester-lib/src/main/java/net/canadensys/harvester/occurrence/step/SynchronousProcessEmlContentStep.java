package net.canadensys.harvester.occurrence.step;

import java.util.Map;

import net.canadensys.dataportal.occurrence.model.ResourceInformationModel;
import net.canadensys.harvester.ItemProcessorIF;
import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.exception.WriterException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.gbif.metadata.eml.Eml;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Step reading an EML file from a DarwinCore archive, process it, writing the result.
 * NOT thread safe
 * 
 * @author canadensys
 * 
 */
public class SynchronousProcessEmlContentStep implements ProcessingStepIF {

	@Autowired
	@Qualifier("dwcaEmlReader")
	private ItemReaderIF<Eml> reader;

	@Autowired
	@Qualifier("resourceInformationWriter")
	private ItemWriterIF<ResourceInformationModel> writer;

	@Autowired
	@Qualifier("resourceInformationProcessor")
	private ItemProcessorIF<Eml, ResourceInformationModel> resourceInformationProcessor;

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
		Eml emlModel = reader.read();
		ResourceInformationModel resourceInformationModel = resourceInformationProcessor.process(emlModel, sharedParameters);

		try {
			writer.write(resourceInformationModel);
		}
		catch (WriterException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getTitle() {
		return "Process EML";
	}
}
