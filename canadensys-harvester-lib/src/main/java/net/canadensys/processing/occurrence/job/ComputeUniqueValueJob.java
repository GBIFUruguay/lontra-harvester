package net.canadensys.processing.occurrence.job;

import net.canadensys.processing.AbstractProcessingJob;
import net.canadensys.processing.ItemTaskIF;
import net.canadensys.processing.occurrence.task.ComputeUniqueValueTask;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * Job to compute the unique values and their counts from the current content of the database.
 * Never run this job in parallel.
 * This job should be replaced by an ElasticSearch index eventually.
 * @author canadensys
 *
 */
public class ComputeUniqueValueJob extends AbstractProcessingJob{

	@Autowired
	private ItemTaskIF computeUniqueValueTask;
	
	public void doJob(){
		computeUniqueValueTask.execute(null);
	}
	
	public void setComputeUniqueValueTask(ComputeUniqueValueTask computeUniqueValueTask) {
		this.computeUniqueValueTask = computeUniqueValueTask;
	}
}
