package net.canadensys.harvester.occurrence.model;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.dataportal.occurrence.model.ImportLogModel;

/**
 * Model that encapsulate a DwcaResourceModel and hold status related information.
 *
 * @author cgendreau
 *
 */
public class DwcaResourceStatusModel {

	private final DwcaResourceModel dwcaResourceModel;
	private IPTFeedModel iptFeedModel;
	private ImportLogModel lastImportEvent;

	public DwcaResourceStatusModel(DwcaResourceModel dwcaResourceModel) {
		this.dwcaResourceModel = dwcaResourceModel;
	}

	public IPTFeedModel getIptFeedModel() {
		return iptFeedModel;
	}

	public void setIptFeedModel(IPTFeedModel iptFeedModel) {
		this.iptFeedModel = iptFeedModel;
	}

	public ImportLogModel getLastImportEvent() {
		return lastImportEvent;
	}

	public void setLastImportEvent(ImportLogModel lastImportEvent) {
		this.lastImportEvent = lastImportEvent;
	}

	public DwcaResourceModel getDwcaResourceModel() {
		return dwcaResourceModel;
	}
}
