package net.canadensys.harvester;

import java.util.Map;

import net.canadensys.harvester.exception.TaskExecutionException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

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
