package net.canadensys.harvester.occurrence.model;

import java.util.Date;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;

/**
 * Model that encapsulate a DwcaResourceModel and hold status related information.
 *
 * @author cgendreau
 *
 */
public class DwcaResourceStatusModel {

	private final DwcaResourceModel dwcaResourceModel;
	private Date lastPublishedDate;
	private Date lastHarvestDate;

	public DwcaResourceStatusModel(DwcaResourceModel dwcaResourceModel) {
		this.dwcaResourceModel = dwcaResourceModel;
	}

	/**
	 * Date at when the resource was last published at its original location (e.g. IPT)
	 *
	 * @return last date the resource was published or null
	 */
	public Date getLastPublishedDate() {
		return lastPublishedDate;
	}

	public void setLastPublishedDate(Date lastPublishedDate) {
		this.lastPublishedDate = lastPublishedDate;
	}

	/**
	 *
	 * @return last date the resource was harvested or null
	 */
	public Date getLastHarvestDate() {
		return lastHarvestDate;
	}

	public void setLastHarvestDate(Date lastHarvestDate) {
		this.lastHarvestDate = lastHarvestDate;
	}

	public DwcaResourceModel getDwcaResourceModel() {
		return dwcaResourceModel;
	}


}
