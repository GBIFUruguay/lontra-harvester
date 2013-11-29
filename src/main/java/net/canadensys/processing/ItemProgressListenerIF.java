package net.canadensys.processing;

/**
 * Interface used to be notified of the progress of a task.
 * @author cgendreau
 *
 */
public interface ItemProgressListenerIF {
	public void onProgress(int current,int total);
}
