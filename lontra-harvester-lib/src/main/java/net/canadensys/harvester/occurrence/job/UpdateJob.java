package net.canadensys.harvester.occurrence.job;

import java.util.HashMap;
import java.util.UUID;

import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.task.PostProcessOccurrenceTask;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * This job allows update tasks to be performed, either after update of database
 * records or a post processing operation such as processing record counts.
 *
 * @author Pedro Guimarães
 *
 */
public class UpdateJob extends AbstractProcessingJob {

	@Autowired
	private ItemTaskIF postProcessOccurrenceTask;

	public UpdateJob() {
		super(UUID.randomUUID().toString());
		sharedParameters = new HashMap<SharedParameterEnum, Object>();
	}

	public void doJob(JobStatusModel jobStatusModel) {
		// This task updates record counts and sets resource and publisher names
		// in the occurrences:
		jobStatusModel
		.setCurrentStatusExplanation("Update occurrence fields and record counts");
		postProcessOccurrenceTask.execute(sharedParameters);
	}

	public void setPostProcessOccurrenceTask(
			PostProcessOccurrenceTask postProcessOccurrenceTask) {
		this.postProcessOccurrenceTask = postProcessOccurrenceTask;
	}

	@Override
	public void cancel() {
	}
}
