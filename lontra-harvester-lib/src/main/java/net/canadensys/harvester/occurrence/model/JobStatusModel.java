package net.canadensys.harvester.occurrence.model;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

/**
 * Model responsible for job status related data.
 * It is using a PropertyChangeSupport delegate.
 * Not thread-safe
 *
 * @author canadensys
 *
 */
public class JobStatusModel {

	private final PropertyChangeSupport propertyChangeSupport;

	public enum JobStatus {
		RUNNING(false), DONE(true), ERROR(true), CANCEL(true);
		private boolean jobCompleted;

		private JobStatus(boolean jobCompleted) {
			this.jobCompleted = jobCompleted;
		}

		/**
		 * Is the JobStatus should be considered as a job completed status.
		 *
		 * @param status
		 * @return
		 */
		public boolean isJobCompleted() {
			return jobCompleted;
		}
	};

	public static String CURRENT_STATUS_PROPERTY = "currentStatus";
	public static String CURRENT_STATUS_EXPLANATION_PROPERTY = "currentStatusExplanation";
	public static String CURRENT_JOB_PROGRESS_PROPERTY = "currentJobProgress";

	private String currentJobId;
	private JobStatus currentStatus;
	private String currentStatusExplanation;
	private String currentJobProgress;

	public JobStatusModel() {
		propertyChangeSupport = new PropertyChangeSupport(this);
	}

	/**
	 * @see PropertyChangeSupport
	 * @param listener
	 */
	public void addPropertyChangeListener(PropertyChangeListener listener) {
		propertyChangeSupport.addPropertyChangeListener(listener);
	}

	/**
	 * @see PropertyChangeSupport
	 * @param listener
	 */
	public void removePropertyChangeListener(PropertyChangeListener listener) {
		propertyChangeSupport.removePropertyChangeListener(listener);
	}

	public void setCurrentStatus(JobStatus newStatus) {
		this.currentStatus = newStatus;
		propertyChangeSupport.firePropertyChange(CURRENT_STATUS_PROPERTY, null, newStatus);
	}

	public void setCurrentStatusExplanation(String newStatusExplanation) {
		this.currentStatusExplanation = newStatusExplanation;
		propertyChangeSupport.firePropertyChange(CURRENT_STATUS_EXPLANATION_PROPERTY, null, newStatusExplanation);
	}

	public void setCurrentJobProgress(String newCurrentJobProgress) {
		this.currentJobProgress = newCurrentJobProgress;
		propertyChangeSupport.firePropertyChange(CURRENT_JOB_PROGRESS_PROPERTY, null, newCurrentJobProgress);
	}

	public String getCurrentJobId() {
		return currentJobId;
	}

	public void setCurrentJobId(String currentJobId) {
		this.currentJobId = currentJobId;
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
