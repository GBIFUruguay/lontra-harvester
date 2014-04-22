package net.canadensys.harvester.occurrence.view.model;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

import net.canadensys.harvester.occurrence.model.JobStatusModel;

/**
 * Harvester view model with PropertyChangeSupport.
 * @author canadensys
 *
 */
public class HarvesterViewModel {

	private final PropertyChangeSupport propertyChangeSupport;
	private JobStatusModel currentJobStatusModel;

	private String databaseLocation;

	public HarvesterViewModel(){
		propertyChangeSupport = new PropertyChangeSupport(this);
		//currentStatus = new ApplicationStatus();
	}

	public void addPropertyChangeListener(PropertyChangeListener listener){
		propertyChangeSupport.addPropertyChangeListener(listener);
	}

	public String getDatabaseLocation() {
		return databaseLocation;
	}
	public void setDatabaseLocation(String databaseLocation) {
		this.databaseLocation = databaseLocation;
		propertyChangeSupport.firePropertyChange("databaseLocation", null, databaseLocation);
	}

	/**
	 * Encapsulate the JobStatusModel into this model.
	 * All PropertyChangeListeners will also be registered to the JobStatusModel.
	 * @param currentJobStatusModel
	 */
	public void encapsulateJobStatus(JobStatusModel currentJobStatusModel){
		//TODO we should probably unregister from previous currentJobStatusModel
		this.currentJobStatusModel = currentJobStatusModel;
		currentJobStatusModel.addPropertyChangeListener(propertyChangeSupport.getPropertyChangeListeners()[0]);
	}
}
