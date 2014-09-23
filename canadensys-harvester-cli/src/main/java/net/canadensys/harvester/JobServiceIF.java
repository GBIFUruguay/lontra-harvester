package net.canadensys.harvester;

import net.canadensys.dataportal.occurrence.model.ResourceModel;

public interface JobServiceIF {

	ResourceModel loadResourceModel(String sourcefileid);
}
