package net.canadensys.harvester.occurrence.step.stream;

import net.canadensys.dataportal.occurrence.model.OccurrenceExtensionModel;
import net.canadensys.harvester.ItemProcessorIF;
import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.message.ProcessingMessageIF;

import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Concrete implementation of GenericStreamProcessingStep for OccurrenceExtensionModel.
 * 
 * @author cgendreau
 *
 */
public class StreamDwcExtensionContentStep extends GenericStreamProcessingStep<OccurrenceExtensionModel,OccurrenceExtensionModel> {

	/**
	 * dwcaOccurrenceExtensionReader should be declared as a prototype.
	 */
	@Override
	@Qualifier("dwcaOccurrenceExtensionReader")
	public void setReader(ItemReaderIF<OccurrenceExtensionModel> reader) {
		super.setReader(reader);
	}
	
	@Override
	@Qualifier("jmsWriter")
	public void setWriter(ItemWriterIF<ProcessingMessageIF> writer) {
		super.setWriter(writer);
	}
	
	@Override
	@Qualifier("extLineProcessor")
	public void setDwcaLineProcessor(
			ItemProcessorIF<OccurrenceExtensionModel, OccurrenceExtensionModel> lineProcessor) {
		super.setDwcaLineProcessor(lineProcessor);
	}
	
}
