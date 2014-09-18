package net.canadensys.harvester.occurrence.job;

import java.util.HashMap;

import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.ItemProgressListenerIF;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.LongRunningTaskIF;
import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.model.JobStatusModel.JobStatus;
import net.canadensys.harvester.occurrence.task.CheckHarvestingCompletenessTask;
import net.canadensys.harvester.occurrence.task.CleanBufferTableTask;
import net.canadensys.harvester.occurrence.task.GetResourceInfoTask;
import net.canadensys.harvester.occurrence.task.PrepareDwcaTask;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * This job allows to give a resource ID, stream the content into JMS messages and waiting for completion.
 * At the end of this job, the content of the DarwinCore archive will be in the database as raw and processed data.
 * 
 * The GetResourceInfoTask is optional if you plan to work from directly from an archive or folder. In this case, the DATASET_SHORTNAME will be
 * extracted from the file name which could lead to unwanted behavior since the name of the file could conflict with another resource.
 * @author canadensys
 *
 */
public class ImportDwcaJob extends AbstractProcessingJob implements ItemProgressListenerIF{
	
	//Task and step
	@Autowired
	private ItemTaskIF getResourceInfoTask;
	
	@Autowired
	private ItemTaskIF prepareDwcaTask;
	
	@Autowired
	private ItemTaskIF cleanBufferTableTask;
	
	@Autowired
	private ProcessingStepIF streamEmlContentStep;
	
	@Autowired
	private ProcessingStepIF streamDwcContentStep;
	
	@Autowired
	private LongRunningTaskIF checkProcessingCompletenessTask;
	
	private JobStatusModel jobStatusModel;
	
	public ImportDwcaJob(){
		sharedParameters = new HashMap<SharedParameterEnum, Object>();
	}
		
	/**
	 * Run the actual job.
	 */
	public void doJob(JobStatusModel jobStatusModel){
		this.jobStatusModel = jobStatusModel;
		//share the statusJobModel so step(s) can update it
		sharedParameters.put(SharedParameterEnum.JOB_STATUS_MODEL, jobStatusModel);
		jobStatusModel.setCurrentStatus(JobStatus.RUNNING);
		
		//optional task, could also import a DwcA from a local path but, at your own risk.
		if(getResourceInfoTask != null && sharedParameters.containsKey(SharedParameterEnum.SOURCE_FILE_ID)){
			getResourceInfoTask.execute(sharedParameters);
		}
		
		//TODO move strings to properties file
		jobStatusModel.setCurrentStatusExplanation("Preparing DwcA");
		prepareDwcaTask.execute(sharedParameters);
		
		jobStatusModel.setCurrentStatusExplanation("Cleaning buffer table");
		cleanBufferTableTask.execute(sharedParameters);
		
		jobStatusModel.setCurrentStatusExplanation("Streaming EML");
		executeStepSequentially(streamEmlContentStep, sharedParameters);
		
		jobStatusModel.setCurrentStatusExplanation("Streaming DwcA content");
		executeStepSequentially(streamDwcContentStep, sharedParameters);
		
		jobStatusModel.setCurrentStatusExplanation("Waiting for completion");
		((CheckHarvestingCompletenessTask)checkProcessingCompletenessTask).addItemProgressListenerIF(this);
		checkProcessingCompletenessTask.execute(sharedParameters);
	}
	
	public void setItemProgressListener(ItemProgressListenerIF listener){
		((CheckHarvestingCompletenessTask)checkProcessingCompletenessTask).addItemProgressListenerIF(listener);
	}
	
	public void setGetResourceInfoTask(GetResourceInfoTask getResourceInfoTask){
		this.getResourceInfoTask = getResourceInfoTask;
	}
	
	public void setPrepareDwcaTask(PrepareDwcaTask prepareDwcaTask) {
		this.prepareDwcaTask = prepareDwcaTask;
	}

	public void setCleanBufferTableTask(CleanBufferTableTask cleanBufferTableTask) {
		this.cleanBufferTableTask = cleanBufferTableTask;
	}

	public void setCheckProcessingCompletenessTask(
			CheckHarvestingCompletenessTask checkProcessingCompletenessTask) {
		this.checkProcessingCompletenessTask = checkProcessingCompletenessTask;
	}
	
	@Override
	public void cancel(){
		checkProcessingCompletenessTask.cancel();
	}

	@Override
	public void onProgress(String context, int current, int total) {
		jobStatusModel.setCurrentJobProgress(current+"/"+total);
	}

	@Override
	public void onSuccess() {
		jobStatusModel.setCurrentStatus(JobStatus.DONE);
	}
	
	@Override
	public void onCancel() {
		jobStatusModel.setCurrentStatus(JobStatus.CANCEL);
	}

	@Override
	public void onError(Throwable t) {
		jobStatusModel.setCurrentStatus(JobStatus.ERROR);
		jobStatusModel.setCurrentStatusExplanation(t.getMessage());
	}

}
