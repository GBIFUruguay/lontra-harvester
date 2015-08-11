package net.canadensys.harvester.occurrence.job;

import java.util.HashMap;
import java.util.UUID;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
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

	// Do not Autowired, it will be created dynamically
	private LongRunningTaskIF checkJobStatus;

	private volatile JobStatusModel jobStatusModel;

	public ImportDwcaJob() {
		super(UUID.randomUUID().toString());
		sharedParameters = new HashMap<SharedParameterEnum, Object>();
	}

	@Override
	public void doJob(JobStatusModel jobStatusModel) {

		this.jobStatusModel = jobStatusModel;
		jobStatusModel.setCurrentJobId(getJobId());
		jobStatusModel.setCurrentStatus(JobStatus.RUNNING);

		// get information about resource
		getResourceInfoTask.execute(sharedParameters);

		DwcaResourceModel resourceModel = (DwcaResourceModel) sharedParameters.get(SharedParameterEnum.RESOURCE_MODEL);
		jobStatusModel.setCurrentStatusExplanation("Working on resource " + resourceModel.getId() + ":" + resourceModel.getName());

		// TODO move strings to properties file
		jobStatusModel.setCurrentStatusExplanation("Preparing Dwc-A");
		prepareDwcaTask.execute(sharedParameters);

		jobStatusModel.setCurrentStatusExplanation("Cleaning buffer table");
		cleanBufferTableTask.execute(sharedParameters);

		jobStatusModel.setCurrentStatusExplanation("Streaming EML");
		executeStepSequentially(streamEmlContentStep, sharedParameters);

		jobStatusModel.setCurrentStatusExplanation("Streaming DwcA content");
		StepResult dwcContent = executeStepSequentially(streamDwcContentStep, sharedParameters);

		jobStatusModel.setCurrentStatusExplanation("Checking for DwcA extension(s)");
		StepResult dwcExtContent = executeStepSequentially(handleDwcaExtensionsStep, sharedParameters);

		jobStatusModel.setCurrentStatusExplanation("Waiting for completion");

		checkJobStatus = createCheckCompletenessTask(dwcContent.getNumberOfRecord(), dwcExtContent.getNumberOfRecord());
		checkJobStatus.execute(sharedParameters);
	}

	/**
	 * Dynamically create ItemTaskIF
	 *
	 * @param targetedTable
	 * @param numberOfRecords
	 * @return
	 */
	public LongRunningTaskIF createCheckCompletenessTask(int numberOfOccurrenceRecords, int numberOfExtensionRecords) {
		CheckHarvestingCompletenessTask chcTask = (CheckHarvestingCompletenessTask) appContext.getBean("checkProcessingCompletenessTask");
		chcTask.addItemProgressListenerIF(this);
		chcTask.addTarget("occurrence_raw", new Integer(numberOfOccurrenceRecords));

		if (numberOfExtensionRecords > 0) {
			chcTask.addTarget("occurrence_extension", new Integer(numberOfExtensionRecords));
		}
		return chcTask;
	}

	public void setGetResourceInfoTask(GetResourceInfoTask getResourceInfoTask) {
		this.getResourceInfoTask = getResourceInfoTask;
	}

	public void setPrepareDwcaTask(PrepareDwcaTask prepareDwcaTask) {
		this.prepareDwcaTask = prepareDwcaTask;
	}

	@Override
	public void cancel() {
		// this is the only step/task that implements 'cancel'
		if (checkJobStatus != null) {
			checkJobStatus.cancel();
		}
	}

	@Override
	public void onProgress(String context, int current, int total) {
		jobStatusModel.setCurrentJobProgress(current + "/" + total);
	}

	@Override
	public void onSuccess(String context) {
		// jobStatusModel.setCurrentStatus(JobStatus.DONE);
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

	@Override
	public void onCompletion() {
		jobStatusModel.setCurrentStatus(JobStatus.DONE);
	}

}
