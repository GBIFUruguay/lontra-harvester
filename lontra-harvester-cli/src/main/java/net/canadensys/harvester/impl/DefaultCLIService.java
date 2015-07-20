package net.canadensys.harvester.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.canadensys.dataportal.occurrence.dao.DwcaResourceDAO;
import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.CLIService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

public class DefaultCLIService implements CLIService {

	@Autowired
	private DwcaResourceDAO resourceDAO;

	@Override
	@Transactional("publicTransactionManager")
	public DwcaResourceModel loadResourceModel(String sourcefileid) {
		return resourceDAO.loadBySourceFileId(sourcefileid);
	}

	@Override
	@Transactional("publicTransactionManager")
	public List<DwcaResourceModel> loadResourceModelList() {
		List<DwcaResourceModel> resourceList = resourceDAO.loadResources();
		Collections.sort(resourceList, new Comparator<DwcaResourceModel>() {
			@Override
			public int compare(DwcaResourceModel o1, DwcaResourceModel o2) {
				return o1.getId().compareTo(o2.getId());
			}
		});
		return resourceList;
	}

}
