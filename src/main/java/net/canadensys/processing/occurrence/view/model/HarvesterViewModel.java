package net.canadensys.processing.occurrence.view.model;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

import net.canadensys.processing.occurrence.model.ApplicationStatus;

/**
 * Harvester view model with PropertyChangeSupport.
 * @author canadensys
 *
 */
public class HarvesterViewModel {

	private PropertyChangeSupport propertyChangeSupport;
	private ApplicationStatus currentStatus;
	private String databaseLocation;
	
	public HarvesterViewModel(){
		propertyChangeSupport = new PropertyChangeSupport(this);
		currentStatus = new ApplicationStatus();
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

	public ApplicationStatus getCurrentStatus() {
		return currentStatus;
	}
	
	public void setImportStatus(ApplicationStatus.JobStatusEnum newStatus){
		currentStatus.setImportStatus(newStatus);
		propertyChangeSupport.firePropertyChange("applicationStatus", null, currentStatus);
	}
	
	public void setMoveStatus(ApplicationStatus.JobStatusEnum newStatus){
		currentStatus.setMoveStatus(newStatus);
		propertyChangeSupport.firePropertyChange("applicationStatus", null, currentStatus);
	}
}
