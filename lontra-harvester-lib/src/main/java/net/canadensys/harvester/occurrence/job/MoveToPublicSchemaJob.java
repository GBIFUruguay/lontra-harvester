package net.canadensys.harvester.occurrence.job;

import java.util.HashMap;
import java.util.UUID;

import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.model.JobStatusModel.JobStatus;
import net.canadensys.harvester.occurrence.task.ComputeGISDataTask;
import net.canadensys.harvester.occurrence.task.PostProcessOccurrenceTask;
import net.canadensys.harvester.occurrence.task.RecordImportTask;
import net.canadensys.harvester.occurrence.task.ReplaceOldOccurrenceTask;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * This job allows to move all the data from the buffer schema to the public
 * one. We are creating the GIS related data inside that step.
 *
 * @author canadensys
 *
 */
public class MoveToPublicSchemaJob extends AbstractProcessingJob {

	@Autowired
	private ItemTaskIF getResourceInfoTask;

	@Autowired
	private ItemTaskIF computeGISDataTask;

	@Autowired
	private ItemTaskIF computeMultimediaDataTask;

	@Autowired
	private ItemTaskIF replaceOldOccurrenceTask;

	@Autowired
	private ItemTaskIF recordImportTask;

	@Autowired
	private ItemTaskIF postProcessOccurrenceTask;

	public MoveToPublicSchemaJob() {
		super(UUID.randomUUID().toString());
		sharedParameters = new HashMap<SharedParameterEnum, Object>();
	}

	public void doJob(JobStatusModel jobStatusModel) {

		getResourceInfoTask.execute(sharedParameters);
		jobStatusModel.setCurrentJobId(getJobId());
		jobStatusModel.setCurrentStatus(JobStatus.RUNNING);

		jobStatusModel.setCurrentStatusExplanation("Compute GIS data");
		computeGISDataTask.execute(sharedParameters);

		jobStatusModel.setCurrentStatusExplanation("Compute multimedia data");
		computeMultimediaDataTask.execute(sharedParameters);

		// This task updates record counts and sets resource and publisher names in the occurrences:
		jobStatusModel.setCurrentStatusExplanation("Update occurrence fields and record count");
		postProcessOccurrenceTask.execute(sharedParameters);

		jobStatusModel.setCurrentStatusExplanation("Replace previous records");
		replaceOldOccurrenceTask.execute(sharedParameters);

		// log the import event
		jobStatusModel.setCurrentStatusExplanation("Log import event");
		recordImportTask.execute(sharedParameters);

		jobStatusModel.setCurrentStatus(JobStatus.DONE);
	}

	public void setComputeGISDataTask(ComputeGISDataTask computeGISDataTask) {
		this.computeGISDataTask = computeGISDataTask;
	}

	public void setReplaceOldOccurrenceTask(ReplaceOldOccurrenceTask replaceOldOccurrenceTask) {
		this.replaceOldOccurrenceTask = replaceOldOccurrenceTask;
	}

	public void setRecordImportTask(RecordImportTask recordImportTask) {
		this.recordImportTask = recordImportTask;
	}

	public void setPostProcessOccurrenceTask(PostProcessOccurrenceTask postProcessOccurrenceTask) {
		this.postProcessOccurrenceTask = postProcessOccurrenceTask;
	}

	@Override
	public void cancel() {
	}

}
