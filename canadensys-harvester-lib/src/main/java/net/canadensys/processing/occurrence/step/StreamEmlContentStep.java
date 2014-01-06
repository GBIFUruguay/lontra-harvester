package net.canadensys.processing.occurrence.step;

import java.util.Calendar;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.ResourceContactModel;
import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.ItemReaderIF;
import net.canadensys.processing.ItemWriterIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.message.ProcessingMessageIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.message.SaveResourceContactMessage;

import org.gbif.metadata.eml.Eml;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Step reading an EML file from a DarwinCore archive, process it, writing the result as a ProcessingMessageIF.
 * NOT thread safe
 * @author canadensys
 *
 */
public class StreamEmlContentStep implements ProcessingStepIF{

	@Autowired
	@Qualifier("dwcaEmlReader")
	private ItemReaderIF<Eml> reader;
	
	@Autowired
	@Qualifier("jmsWriter")
	private ItemWriterIF<ProcessingMessageIF> writer;
	
	@Autowired
	@Qualifier("resourceContactProcessor")
	private ItemProcessorIF<Eml, ResourceContactModel> resourceContactProcessor;
	
	private Map<SharedParameterEnum,Object> sharedParameters;
	
	@Override
	public void preStep(Map<SharedParameterEnum, Object> sharedParameters){
		if(writer == null){
			throw new IllegalStateException("No writer defined");
		}
		if(resourceContactProcessor == null){
			throw new IllegalStateException("No processor defined");
		}
		if(reader == null){
			throw new IllegalStateException("No reader defined");
		}
		this.sharedParameters = sharedParameters;
		reader.openReader(sharedParameters);
		writer.openWriter();
		resourceContactProcessor.init();
	}

	@Override
	public void postStep() {
		writer.closeWriter();
		resourceContactProcessor.destroy();
		reader.closeReader();
	}

	@Override
	public void doStep() {
		//For now, we only read and stream the resource contact
		SaveResourceContactMessage srcm = new SaveResourceContactMessage();
		srcm.setWhen(Calendar.getInstance().getTime().toString());
		
		Eml emlModel = reader.read();
		ResourceContactModel resourceContactModel = resourceContactProcessor.process(emlModel, sharedParameters);
		
		srcm.setResourceContactModel(resourceContactModel);

		writer.write(srcm);
	}
}
