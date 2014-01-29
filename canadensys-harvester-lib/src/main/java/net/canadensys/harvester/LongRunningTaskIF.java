package net.canadensys.harvester;

/**
 * Common interface for long running item task that can be canceled at any moment without
 * leaving data in inconsistent state.
 * @author canadensys
 *
 */
public interface LongRunningTaskIF extends ItemTaskIF {
	
	/**
	 * Cancel the task long running task execution.
	 */
	public void cancel();
}
