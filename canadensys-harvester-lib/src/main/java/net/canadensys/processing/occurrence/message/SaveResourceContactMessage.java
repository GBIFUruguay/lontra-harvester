package net.canadensys.processing.occurrence.message;

import net.canadensys.dataportal.occurrence.model.ResourceContactModel;
import net.canadensys.processing.message.ProcessingMessageIF;

/**
 * Message asking to insert or update Resource Contact data.
 * @author canadensys
 *
 */
public class SaveResourceContactMessage implements ProcessingMessageIF{
	private String when;
	
	private ResourceContactModel resourceContactModel;

	public String getWhen() {
		return when;
	}
	public void setWhen(String when) {
		this.when = when;
	}
	
	public ResourceContactModel getResourceContactModel() {
		return resourceContactModel;
	}
	public void setResourceContactModel(ResourceContactModel resourceContactModel) {
		this.resourceContactModel = resourceContactModel;
	}
}
