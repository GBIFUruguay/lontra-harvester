package net.canadensys.harvester.impl;

import net.canadensys.dataportal.occurrence.dao.ResourceDAO;
import net.canadensys.dataportal.occurrence.model.ResourceModel;
import net.canadensys.harvester.JobServiceIF;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

public class JobServiceImpl implements JobServiceIF{

	@Autowired
	private ResourceDAO resourceDAO;
	
	@Override
	@Transactional("publicTransactionManager")
	public ResourceModel loadResourceModel(String sourcefileid){
		return resourceDAO.load(sourcefileid);
	}
	
}
