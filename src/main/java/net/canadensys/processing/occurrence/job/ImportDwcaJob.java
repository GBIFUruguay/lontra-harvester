package net.canadensys.processing.occurrence.job;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.processing.AbstractProcessingJob;
import net.canadensys.processing.ItemProgressListenerIF;
import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.task.CheckProcessingCompletenessTask;
import net.canadensys.processing.occurrence.task.CleanBufferTableTask;
import net.canadensys.processing.occurrence.task.GetResourceInfoTask;
import net.canadensys.processing.occurrence.task.PrepareDwcaTask;

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
		//optional task
		if(getResourceInfoTask != null){
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

}
