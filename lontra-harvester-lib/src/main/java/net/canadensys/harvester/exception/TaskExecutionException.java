package net.canadensys.harvester.exception;

import net.canadensys.harvester.ItemTaskIF;

/**
 * Exception that could be thrown while running a task
 * 
 * @see ItemTaskIF
 * @author canadensys
 * 
 */
public class TaskExecutionException extends RuntimeException {

	private static final long serialVersionUID = 3119417498139887680L;

	public TaskExecutionException(String message) {
		super(message);
	}

	public TaskExecutionException(String message, Throwable cause) {
		super(message, cause);
	}
}
