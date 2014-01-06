package net.canadensys.processing.occurrence.controller;

import java.util.List;

import net.canadensys.processing.ItemProgressListenerIF;
import net.canadensys.processing.occurrence.model.IPTFeedModel;
import net.canadensys.processing.occurrence.model.ImportLogModel;
import net.canadensys.processing.occurrence.model.ResourceModel;

import com.google.common.util.concurrent.FutureCallback;

public interface StepControllerIF extends FutureCallback<Void>{
	
	public void registerProgressListener(ItemProgressListenerIF progressListener);
	public void importDwcA(Integer resourceId);
	public void moveToPublicSchema(String datasetShortName);
	
	public List<ResourceModel> getResourceModelList();
	public List<ImportLogModel> getSortedImportLogModelList();
	public List<IPTFeedModel> getIPTFeed();
}
