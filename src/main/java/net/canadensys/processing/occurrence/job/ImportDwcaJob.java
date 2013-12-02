package net.canadensys.processing.occurrence.job;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.processing.ItemProgressListenerIF;
import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.step.StreamDwcaContentStep;
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
public class ImportDwcaJob{
	
	protected Map<SharedParameterEnum,Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
	
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
	private ProcessingStepIF streamDwcaContentStep;
	
	@Autowired
	private ItemTaskIF checkProcessingCompletenessTask;
	
	public void addToSharedParameters(SharedParameterEnum key, Object obj){
		sharedParameters.put(key, obj);
	}
	
	public Object getSharedParameter(SharedParameterEnum key){
		return sharedParameters.get(key);
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
		
		try {
			streamEmlContentStep.preStep(sharedParameters);
			streamEmlContentStep.doStep();
			streamEmlContentStep.postStep();
			
			streamDwcaContentStep.preStep(sharedParameters);
			streamDwcaContentStep.doStep();
			streamDwcaContentStep.postStep();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
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

	public void setReadDwcaStep(StreamDwcaContentStep readDwcaStep) {
		this.streamDwcaContentStep = readDwcaStep;
	}

	public void setProcessingCompletion(
			CheckProcessingCompletenessTask processingCompletion) {
		this.checkProcessingCompletenessTask = processingCompletion;
	}

}
