package net.canadensys.processing.occurrence.job;

import java.util.HashMap;

import net.canadensys.processing.AbstractProcessingJob;
import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.task.ComputeGISDataTask;
import net.canadensys.processing.occurrence.task.RecordImportTask;
import net.canadensys.processing.occurrence.task.ReplaceOldOccurrenceTask;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * This job allows to move all the data from the buffer schema to the public one. We are creating the GIS related data inside that step.
 * @author canadensys
 *
 */
public class MoveToPublicSchemaJob extends AbstractProcessingJob{

	@Autowired
	private ItemTaskIF computeGISDataTask;
	
	@Autowired
	private ItemTaskIF replaceOldOccurrenceTask;
	
	@Autowired
	private ItemTaskIF recordImportTask;
	
	public MoveToPublicSchemaJob(){
		sharedParameters = new HashMap<SharedParameterEnum, Object>();
	}
	
	public void doJob(){
		
		computeGISDataTask.execute(sharedParameters);
		replaceOldOccurrenceTask.execute(sharedParameters);
		
		//log the import event
		recordImportTask.execute(sharedParameters);
	}

	public void setComputeGISDataTask(ComputeGISDataTask computeGISDataTask) {
		this.computeGISDataTask = computeGISDataTask;
	}

	public void setReplaceOldOccurrenceTask(
			ReplaceOldOccurrenceTask replaceOldOccurrenceTask) {
		this.replaceOldOccurrenceTask = replaceOldOccurrenceTask;
	}

	public void setRecordImportTask(RecordImportTask recordImportTask) {
		this.recordImportTask = recordImportTask;
	}

}
