package net.canadensys.harvester.impl;

import net.canadensys.dataportal.occurrence.dao.DwcaResourceDAO;
import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.JobServiceIF;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

public class JobServiceImpl implements JobServiceIF {

	@Autowired
	private DwcaResourceDAO resourceDAO;

	@Override
	@Transactional("publicTransactionManager")
	public DwcaResourceModel loadResourceModel(String sourcefileid) {
		return resourceDAO.loadBySourceFileId(sourcefileid);
	}

}
