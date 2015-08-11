package net.canadensys.harvester;

import java.util.Map;

import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.model.JobStatusModel;

/**
 * Common functions for Job processing classes.
 *
 * @author canadensys
 *
 */
public abstract class AbstractProcessingJob {

	private final String jobId;

	// Data structure used to share parameters among steps
	protected Map<SharedParameterEnum, Object> sharedParameters;

	protected AbstractProcessingJob(String jobId) {
		this.jobId = jobId;
	}

	// this could be needed to allow steps insertion
	// protected List<ProcessingStepIF> stepSequence;
	// insertStep(StepDefinitionID insertionPoint, ProcessingStepIF step)

	/**
	 * This function assumes that sharedParameters was previously created by the child class.
	 * NullPointerException will be thrown if sharedParameters wasn't created.
	 *
	 * @param key
	 * @param obj
	 */
	public void addToSharedParameters(SharedParameterEnum key, Object obj) {
		sharedParameters.put(key, obj);
	}

	/**
	 * This function assumes that sharedParameters was previously created by the child class.
	 * NullPointerException will be thrown if sharedParameters wasn't created.
	 *
	 * @param key
	 * @return
	 */
	public Object getFromSharedParameters(SharedParameterEnum key) {
		return sharedParameters.get(key);
	}

	public String getJobId() {
		return jobId;
	}

	/**
	 * Execute a step sequentially (preStep,doStep and postStep).
	 * TODO allow steps to return result for better error reporting
	 *
	 * @param step
	 * @param sharedParameters
	 * @throws IllegalStateException
	 *             from ProcessingStepIF.preStep
	 */
	protected StepResult executeStepSequentially(StepIF step, Map<SharedParameterEnum, Object> sharedParameters) throws IllegalStateException {
		step.preStep(sharedParameters);
		StepResult stepReturn = step.doStep();
		step.postStep();
		return stepReturn;
	}

	/**
	 * Cancel the job
	 */
	public abstract void cancel();

	/**
	 * Run the actual job.
	 *
	 * @param jobStatusModel
	 *            Object to hold the status of the job. It's a status object so it will be updated by this method.
	 */
	public abstract void doJob(JobStatusModel jobStatusModel);

}
