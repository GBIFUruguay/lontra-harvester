package net.canadensys.harvester.occurrence.controller;

import java.util.List;

import net.canadensys.harvester.ItemProgressListenerIF;
import net.canadensys.harvester.occurrence.model.IPTFeedModel;
import net.canadensys.harvester.occurrence.model.ImportLogModel;
import net.canadensys.harvester.occurrence.model.ResourceModel;

import com.google.common.util.concurrent.FutureCallback;

public interface StepControllerIF extends FutureCallback<Void> {

	public List<IPTFeedModel> getIPTFeed();

	public List<ResourceModel> getResourceModelList();

	public List<ImportLogModel> getSortedImportLogModelList();

	public void importDwcA(Integer resourceId);

	public void moveToPublicSchema(String datasetShortName);

	public void registerProgressListener(ItemProgressListenerIF progressListener);

	/**
	 * Insert or update a ResourceModel.
	 * 
	 * @param resourceModel
	 * @return
	 */
	public boolean updateResourceModel(ResourceModel resourceModel);
}
