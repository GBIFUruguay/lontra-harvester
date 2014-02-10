package net.canadensys.harvester;

import java.util.Map;

import net.canadensys.harvester.occurrence.SharedParameterEnum;

/**
 * Wrapper used to expose a ItemTaskIF as a ProcessingStepIF.
 * @author canadensys
 *
 */
public class TaskStepWrapper implements ProcessingStepIF{

	private Map<SharedParameterEnum, Object> sharedParameters;
	
	//wrapped task
	private ItemTaskIF task;
	
	//Default constructor to allow Java Beans usage
	public TaskStepWrapper(){}
	
	public TaskStepWrapper(ItemTaskIF task){
		this.task = task;
	}
	
	public void setTask(ItemTaskIF task){
		this.task = task;
	}
	
	@Override
	public void preStep(Map<SharedParameterEnum, Object> sharedParameters)
			throws IllegalStateException {
		this.sharedParameters = sharedParameters;
	}
	
	@Override
	public void doStep() {
		task.execute(sharedParameters);
	}
	
	@Override
	public void postStep() {
		// no op
	}

}
