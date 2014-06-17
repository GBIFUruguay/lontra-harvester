package net.canadensys.harvester.occurrence.dao;

import java.util.List;

import net.canadensys.harvester.occurrence.model.ResourceModel;

public interface ResourceDAO {

	public List<ResourceModel> loadResources();
}
