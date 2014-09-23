package net.canadensys.harvester.occurrence.step.async;

import net.canadensys.dataportal.occurrence.model.OccurrenceExtensionModel;
import net.canadensys.harvester.ItemWriterIF;
import net.canadensys.harvester.jms.control.JMSControlProducer;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * No processor for now but it will come
 * @author cgendreau
 *
 */
public class AsyncManageOccurrenceExtensionStep extends GenericAsyncStep<OccurrenceExtensionModel> {
	
	@Autowired
	private JMSControlProducer errorReporter;

	public AsyncManageOccurrenceExtensionStep() {
		super(OccurrenceExtensionModel.class);
	}
	
	public void setWriter(ItemWriterIF<OccurrenceExtensionModel> writer) {
		super.setWriter(writer);
	}

}
