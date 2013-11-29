package net.canadensys.processing;

import java.util.Map;

import net.canadensys.processing.exception.TaskExecutionException;
import net.canadensys.processing.occurrence.SharedParameterEnum;

/**
 * Common interface for item task
 * @author canadensys
 *
 */
public interface ItemTaskIF {
	/**
	 * Run this task
	 * @param sharedParameters Shared parameters among different tasks or steps
	 * @exception if something goes wrong
	 */
	public void execute(Map<SharedParameterEnum,Object> sharedParameters) throws TaskExecutionException;
}
