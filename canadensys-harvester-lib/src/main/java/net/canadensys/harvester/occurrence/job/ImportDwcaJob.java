package net.canadensys.harvester.occurrence.job;

import java.util.HashMap;

import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.ItemProgressListenerIF;
import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.task.CheckProcessingCompletenessTask;
import net.canadensys.harvester.occurrence.task.CleanBufferTableTask;
import net.canadensys.harvester.occurrence.task.GetResourceInfoTask;
import net.canadensys.harvester.occurrence.task.PrepareDwcaTask;

import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.util.concurrent.FutureCallback;

/**
 * This job allows to give a resource ID, stream the content into JMS messages and waiting for completion.
 * At the end of this job, the content of the DarwinCore archive will be in the database as raw and processed data.
 * 
 * The GetResourceInfoTask is optional if you plan to work from directly from an archive or folder. In this case, the DATASET_SHORTNAME will be
 * extracted from the file name which could lead to unwanted behavior since the name of the file could conflict with another resource.
 * @author canadensys
 *
 */
public class ImportDwcaJob extends AbstractProcessingJob{
	
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
	private ItemTaskIF checkProcessingCompletenessTask;
	
	public ImportDwcaJob(){
		sharedParameters = new HashMap<SharedParameterEnum, Object>();
	}
		
	/**
	 * Run the actual job
	 */
	public void doJob(FutureCallback<Void> jobCallback){
		//optional task, could also import a DwcA from a local path but, at your own risk.
		if(getResourceInfoTask != null && sharedParameters.containsKey(SharedParameterEnum.RESOURCE_ID)){
			getResourceInfoTask.execute(sharedParameters);
		}
		
		prepareDwcaTask.execute(sharedParameters);
		cleanBufferTableTask.execute(sharedParameters);
		
		executeStepSequentially(streamEmlContentStep, sharedParameters);
		executeStepSequentially(streamDwcContentStep, sharedParameters);
		
		sharedParameters.put(SharedParameterEnum.CALLBACK,jobCallback);
		checkProcessingCompletenessTask.execute(sharedParameters);
	}
	
	public void setItemProgressListener(ItemProgressListenerIF listener){
		((CheckProcessingCompletenessTask)checkProcessingCompletenessTask).addItemProgressListenerIF(listener);
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
			CheckProcessingCompletenessTask checkProcessingCompletenessTask) {
		this.checkProcessingCompletenessTask = checkProcessingCompletenessTask;
	}
	
	@Override
	public void cancel(){
	}

}
