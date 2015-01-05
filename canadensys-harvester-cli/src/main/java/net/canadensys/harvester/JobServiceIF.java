package net.canadensys.harvester;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;

public interface JobServiceIF {

	DwcaResourceModel loadResourceModel(String sourcefileid);
}
