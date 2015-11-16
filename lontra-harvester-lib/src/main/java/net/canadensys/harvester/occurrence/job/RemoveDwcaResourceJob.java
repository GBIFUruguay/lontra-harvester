package net.canadensys.harvester.occurrence.job;

import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;

import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.model.JobStatusModel.JobStatus;
import net.canadensys.harvester.occurrence.task.RemoveDwcaResourceTask;

public class RemoveDwcaResourceJob  extends AbstractProcessingJob {

	@Autowired
	private ItemTaskIF removeDwcaResourceTask;
	
	private Map<SharedParameterEnum, Object> sharedParameters;

	public RemoveDwcaResourceJob() {
		super(UUID.randomUUID().toString());
	}

	@Override
	public void doJob(JobStatusModel jobStatusModel) {
		jobStatusModel.setCurrentJobId(getJobId());
		jobStatusModel.setCurrentStatus(JobStatus.RUNNING);
		jobStatusModel.setCurrentStatusExplanation("Removing dwca resource...");
		removeDwcaResourceTask.execute(sharedParameters);
		jobStatusModel.setCurrentStatus(JobStatus.DONE);
	}

	public void setRemoveDwcaResourceTask(RemoveDwcaResourceTask removeDwcaResourceTask) {
		this.removeDwcaResourceTask = removeDwcaResourceTask;
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
