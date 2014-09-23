package net.canadensys.harvester.occurrence.message;

import net.canadensys.dataportal.occurrence.model.ResourceInformationModel;
import net.canadensys.harvester.message.ProcessingMessageIF;

/**
 * Message asking to insert or update Resource Information data.
 * 
 * @author canadensys
 * 
 */
public class SaveResourceInformationMessage implements ProcessingMessageIF {
	private String when;

	private ResourceInformationModel resourceInformationModel;

	public String getWhen() {
		return when;
	}

	public void setWhen(String when) {
		this.when = when;
	}

	public ResourceInformationModel getResourceInformationModel() {
		return resourceInformationModel;
	}

	public void setResourceInformationModel(ResourceInformationModel resourceInformationModel) {
		this.resourceInformationModel = resourceInformationModel;
	}
}
