package net.canadensys.harvester.occurrence.message;

import java.util.List;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.harvester.message.ProcessingMessageIF;
import net.canadensys.harvester.model.BulkDataObject;

/**
 * Message asking to process raw occurrence data.
 * 
 * @author canadensys
 * 
 */
public class ProcessOccurrenceMessage implements ProcessingMessageIF {
	private String when;

	private BulkDataObject<OccurrenceRawModel> bulkRawModel;

	public ProcessOccurrenceMessage() {
	}

	public ProcessOccurrenceMessage(List<String> occurrenceRawModelProperties) {
		bulkRawModel = new BulkDataObject<OccurrenceRawModel>(occurrenceRawModelProperties);
	}

	public String getWhen() {
		return when;
	}

	public void setWhen(String when) {
		this.when = when;
	}

	public void addRawModel(OccurrenceRawModel rawModel) {
		bulkRawModel.addObject(rawModel);
	}

	public BulkDataObject<OccurrenceRawModel> getBulkRawModel() {
		return bulkRawModel;
	}

	public void setBulkRawModel(BulkDataObject<OccurrenceRawModel> bulkRawModel) {
		this.bulkRawModel = bulkRawModel;
	}
}
