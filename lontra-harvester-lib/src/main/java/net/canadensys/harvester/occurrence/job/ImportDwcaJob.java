package net.canadensys.harvester.occurrence.job;

import java.util.HashMap;
import java.util.UUID;

import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.ItemProgressListenerIF;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.LongRunningTaskIF;
import net.canadensys.harvester.StepIF;
import net.canadensys.harvester.StepResult;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.model.JobStatusModel.JobStatus;
import net.canadensys.harvester.occurrence.task.CheckHarvestingCompletenessTask;
import net.canadensys.harvester.occurrence.task.GetResourceInfoTask;
import net.canadensys.harvester.occurrence.task.PrepareDwcaTask;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

/**
 * This job allows to give a resource ID, stream the content into JMS messages and waiting for completion.
 * At the end of this job, the content of the DarwinCore archive will be in the database as raw and processed data.
 *
 * @author canadensys
 *
 */
public class ImportDwcaJob extends AbstractProcessingJob implements ItemProgressListenerIF {

	@Autowired
	private ApplicationContext appContext;

	// Task and step
	@Autowired
	private ItemTaskIF getResourceInfoTask;

	@Autowired
	private ItemTaskIF prepareDwcaTask;

	@Autowired
	private ItemTaskIF cleanBufferTableTask;

	@Autowired
	private StepIF streamEmlContentStep;

	@Autowired
	private StepIF streamDwcContentStep;

	@Autowired
	private StepIF handleDwcaExtensionsStep;

	@Autowired
	private LongRunningTaskIF checkProcessingCompletenessTask;

	private JobStatusModel jobStatusModel;

	public ImportDwcaJob() {
		super(UUID.randomUUID().toString());
		sharedParameters = new HashMap<SharedParameterEnum, Object>();
	}

	/**
	 * Run the actual job.
	 */
	public void doJob(JobStatusModel jobStatusModel) {

		this.jobStatusModel = jobStatusModel;
		jobStatusModel.setCurrentJobId(getJobId());
		jobStatusModel.setCurrentStatus(JobStatus.RUNNING);

		// get information about resource
		getResourceInfoTask.execute(sharedParameters);

		// TODO move strings to properties file
		jobStatusModel.setCurrentStatusExplanation("Preparing DwcA");
		prepareDwcaTask.execute(sharedParameters);

		jobStatusModel.setCurrentStatusExplanation("Cleaning buffer table");
		cleanBufferTableTask.execute(sharedParameters);

		jobStatusModel.setCurrentStatusExplanation("Streaming EML");
		executeStepSequentially(streamEmlContentStep, sharedParameters);

		jobStatusModel.setCurrentStatusExplanation("Streaming DwcA content");
		StepResult dwcContent = executeStepSequentially(streamDwcContentStep, sharedParameters);

		jobStatusModel.setCurrentStatusExplanation("Checking for DwcA extension(s)");
		executeStepSequentially(handleDwcaExtensionsStep, sharedParameters);

		jobStatusModel.setCurrentStatusExplanation("Waiting for completion");

		ItemTaskIF checkOccurrenceRecords = createCheckCompletenessTask("occurrence_raw",
				dwcContent.getNumberOfRecord());
		checkOccurrenceRecords.execute(sharedParameters);
	}

	/**
	 * Dynamically create ItemTaskIF
	 *
	 * @param context
	 * @param identifier
	 * @param numberOfRecords
	 * @return
	 */
	public ItemTaskIF createCheckCompletenessTask(String context, int numberOfRecords) {
		CheckHarvestingCompletenessTask chcTask = (CheckHarvestingCompletenessTask) appContext.getBean("checkProcessingCompletenessTask");
		chcTask.addItemProgressListenerIF(this);
		chcTask.configure("occurrence_raw", new Integer(numberOfRecords));
		return chcTask;
	}

	public void setItemProgressListener(ItemProgressListenerIF listener) {
		((CheckHarvestingCompletenessTask) checkProcessingCompletenessTask).addItemProgressListenerIF(listener);
	}

	public void setGetResourceInfoTask(GetResourceInfoTask getResourceInfoTask) {
		this.getResourceInfoTask = getResourceInfoTask;
	}

	public void setPrepareDwcaTask(PrepareDwcaTask prepareDwcaTask) {
		this.prepareDwcaTask = prepareDwcaTask;
	}

	public void setCheckProcessingCompletenessTask(CheckHarvestingCompletenessTask checkProcessingCompletenessTask) {
		this.checkProcessingCompletenessTask = checkProcessingCompletenessTask;
	}

	@Override
	public void cancel() {
		checkProcessingCompletenessTask.cancel();
	}

	@Override
	public void onProgress(String context, int current, int total) {
		jobStatusModel.setCurrentJobProgress(current + "/" + total);
	}

	@Override
	public void onSuccess(String context) {
		jobStatusModel.setCurrentStatus(JobStatus.DONE);
	}

	@Override
	public void onCancel(String context) {
		jobStatusModel.setCurrentStatus(JobStatus.CANCEL);
	}

	@Override
	public void onError(String context, Throwable t) {
		jobStatusModel.setCurrentStatus(JobStatus.ERROR);
		jobStatusModel.setCurrentStatusExplanation(t.getMessage());
	}

}
