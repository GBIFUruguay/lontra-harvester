package net.canadensys.harvester.occurrence.job;

import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;

import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.model.JobStatusModel.JobStatus;

public class PublisherNameUpdateJob  extends AbstractProcessingJob {

	@Autowired
	private ItemTaskIF publisherNameUpdateTask;
	
	private Map<SharedParameterEnum, Object> sharedParameters;

	public PublisherNameUpdateJob() {
		super(UUID.randomUUID().toString());
	}

	@Override
	public void doJob(JobStatusModel jobStatusModel) {
		jobStatusModel.setCurrentJobId(getJobId());
		jobStatusModel.setCurrentStatus(JobStatus.RUNNING);
		jobStatusModel.setCurrentStatusExplanation("Updating publisher name on occurrence...");
		publisherNameUpdateTask.execute(sharedParameters);
		jobStatusModel.setCurrentStatus(JobStatus.DONE);
	}

	public void setPublisherNameUpdateTask(ItemTaskIF publisherNameUpdateTask) {
		this.publisherNameUpdateTask = publisherNameUpdateTask;
	}

	public void setSharedParameters(Map<SharedParameterEnum, Object> sharedParameters) {
		this.sharedParameters = sharedParameters;
	}
	
	public Map<SharedParameterEnum, Object> getSharedParamenters() {
		return sharedParameters;
	}
	
	@Override
	public void cancel() {
	}
}
