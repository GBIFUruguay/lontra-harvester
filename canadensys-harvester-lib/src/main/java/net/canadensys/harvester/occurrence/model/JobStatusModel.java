package net.canadensys.harvester.occurrence.model;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

import net.canadensys.harvester.AbstractProcessingJob;

/**
 * Model responsible for job status related data.
 * It is using a PropertyChangeSupport delegate.
 * @author canadensys
 *
 */
public class JobStatusModel {
	private final PropertyChangeSupport propertyChangeSupport;
	
	public enum JobStatus {RUNNING,DONE,ERROR,CANCEL};
	
	public String CURRENT_STATUS_PROPERTY = "currentStatus";
	public String CURRENT_STATUS_EXPLANATION_PROPERTY = "currentStatusExplanation";
	public String CURRENT_JOB_PROGRESS_PROPERTY = "currentJobProgress";
	
	private AbstractProcessingJob currentJob;
	private JobStatus currentStatus;
	private String currentStatusExplanation;
	private String currentJobProgress;
	
	public JobStatusModel(){
		propertyChangeSupport = new PropertyChangeSupport(this);
	}
	
	/**
	 * @see PropertyChangeSupport
	 * @param listener
	 */
	public void addPropertyChangeListener(PropertyChangeListener listener){
		propertyChangeSupport.addPropertyChangeListener(listener);
	}
	
	/**
	 * @see PropertyChangeSupport
	 * @param listener
	 */
	public void removePropertyChangeListener(PropertyChangeListener listener){
		propertyChangeSupport.removePropertyChangeListener(listener);
	}
	
	public void setCurrentStatus(JobStatus newStatus){
		this.currentStatus = newStatus;
		propertyChangeSupport.firePropertyChange(CURRENT_STATUS_PROPERTY, null, newStatus);
	}
	
	public void setCurrentStatusExplanation(String newStatusExplanation){
		this.currentStatusExplanation = newStatusExplanation;
		propertyChangeSupport.firePropertyChange(CURRENT_STATUS_EXPLANATION_PROPERTY, null, newStatusExplanation);
	}
	
	public void setCurrentJobProgress(String newCurrentJobProgress){
		this.currentJobProgress = newCurrentJobProgress;
		propertyChangeSupport.firePropertyChange(CURRENT_JOB_PROGRESS_PROPERTY, null, newCurrentJobProgress);
	}

	public AbstractProcessingJob getCurrentJob() {
		return currentJob;
	}
	public void setCurrentJob(AbstractProcessingJob currentJob) {
		this.currentJob = currentJob;
	}

	public JobStatus getCurrentStatus() {
		return currentStatus;
	}

	public String getCurrentStatusExplanation() {
		return currentStatusExplanation;
	}

	public String getCurrentJobProgress() {
		return currentJobProgress;
	}
}
