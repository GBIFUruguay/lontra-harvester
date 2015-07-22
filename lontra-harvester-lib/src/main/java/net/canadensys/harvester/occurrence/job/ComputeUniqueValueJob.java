package net.canadensys.harvester.occurrence.job;

import java.util.UUID;

import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.model.JobStatusModel.JobStatus;
import net.canadensys.harvester.occurrence.task.ComputeUniqueValueTask;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * Job to compute the unique values and their counts from the current content of the database.
 * Never run this job in parallel.
 * This job should be replaced by an ElasticSearch index eventually.
 *
 * @author canadensys
 *
 */
public class ComputeUniqueValueJob extends AbstractProcessingJob {

	@Autowired
	private ItemTaskIF computeUniqueValueTask;

	public ComputeUniqueValueJob() {
		super(UUID.randomUUID().toString());
	}

	public void doJob(JobStatusModel jobStatusModel) {
		jobStatusModel.setCurrentJobId(getJobId());
		jobStatusModel.setCurrentStatus(JobStatus.RUNNING);
		jobStatusModel.setCurrentStatusExplanation("Compute unique values...");
		computeUniqueValueTask.execute(null);
		jobStatusModel.setCurrentStatus(JobStatus.DONE);
	}

	public void setComputeUniqueValueTask(ComputeUniqueValueTask computeUniqueValueTask) {
		this.computeUniqueValueTask = computeUniqueValueTask;
	}

	@Override
	public void cancel() {
	}
}
