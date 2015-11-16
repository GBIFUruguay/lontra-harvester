package net.canadensys.harvester.occurrence.controller;

import java.util.List;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.dataportal.occurrence.model.ImportLogModel;
import net.canadensys.dataportal.occurrence.model.PublisherModel;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.model.DwcaResourceStatusModel;
import net.canadensys.harvester.occurrence.model.IPTFeedModel;

public interface StepControllerIF {

	public enum JobType {
		IMPORT_DWC, MOVE_TO_PUBLIC, COMPUTE_UNIQUE, GENERIC_JOB;
	};

	public List<IPTFeedModel> getIPTFeed();

	public List<DwcaResourceModel> getResourceModelList();

	public List<PublisherModel> getPublisherModelList();

	/**
	 * Get list of DwcaResourceStatusModel that require to be harvested.
	 *
	 * @return
	 */
	public List<DwcaResourceStatusModel> getResourceToHarvest();

	public List<ImportLogModel> getSortedImportLogModelList();

	/**
	 * Import the specified resource into the buffer schema.
	 *
	 * @param resourceId
	 * @param moveToPublicSchema
	 *            should we automatically 'moveToPublicSchema'
	 * @param computeUniqueValues
	 *            should we automatically 'computeUniqueValues'
	 */
	public void importDwcA(Integer resourceId, boolean moveToPublicSchema, boolean computeUniqueValues);

	public void moveToPublicSchema(Integer resourceID, boolean computeUniqueValues);

	public void computeUniqueValues();

	public void removeDwcaResource(Map<SharedParameterEnum, Object> sharedParameters);

	public void publisherNameUpdate(Map<SharedParameterEnum, Object> sharedParameters);

	public void onNodeError();
	
	public void removePublisher(Map<SharedParameterEnum, Object> sharedParameters);

	/**
	 * Insert or update a ResourceModel.
	 *
	 * @param resourceModel
	 * @return
	 */
	public boolean updateResourceModel(DwcaResourceModel resourceModel);

	/**
	 * Insert or update a PublisherModel
	 *
	 * @param publisherModel
	 * @return
	 */
	public boolean updatePublisherModel(PublisherModel publisherModel);

	public void refreshResource(int resourceId);

	/**
	 * Get JobType from a jobId.
	 *
	 * @param jobId
	 * @return the JobType or null if the jobId is unknown.
	 */
	public JobType getJobType(String jobId);

}
