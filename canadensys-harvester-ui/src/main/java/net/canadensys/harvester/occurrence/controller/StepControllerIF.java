package net.canadensys.harvester.occurrence.controller;

import java.util.List;

import net.canadensys.harvester.ItemProgressListenerIF;
import net.canadensys.harvester.occurrence.model.IPTFeedModel;
import net.canadensys.harvester.occurrence.model.ImportLogModel;
import net.canadensys.harvester.occurrence.model.ResourceModel;

import com.google.common.util.concurrent.FutureCallback;

public interface StepControllerIF extends FutureCallback<Void>{
	
	public void registerProgressListener(ItemProgressListenerIF progressListener);
	public void importDwcA(Integer resourceId);
	public void moveToPublicSchema(String datasetShortName);
	
	public List<ResourceModel> getResourceModelList();
	public List<ImportLogModel> getSortedImportLogModelList();
	public List<IPTFeedModel> getIPTFeed();
}
