package net.canadensys.harvester.occurrence.view.model;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

/**
 * Harvester view model with PropertyChangeSupport.
 * This model is also used to propagate other related PropertyChangeEvent.
 *
 * @author canadensys
 *
 */
public class HarvesterViewModel {

	private final PropertyChangeSupport propertyChangeSupport;

	private String databaseLocation;

	public HarvesterViewModel() {
		propertyChangeSupport = new PropertyChangeSupport(this);
		// jobStatusModelListener = new JobStatusModelListener();
	}

	public void addPropertyChangeListener(PropertyChangeListener listener) {
		propertyChangeSupport.addPropertyChangeListener(listener);
	}

	public void removePropertyChangeListener(PropertyChangeListener listener) {
		propertyChangeSupport.removePropertyChangeListener(listener);
	}

	public String getDatabaseLocation() {
		return databaseLocation;
	}

	public void setDatabaseLocation(String databaseLocation) {
		this.databaseLocation = databaseLocation;
		propertyChangeSupport.firePropertyChange("databaseLocation", null, databaseLocation);
	}

	/**
	 * Propagate external PropertyChangeEvent through this model.
	 *
	 * @param evt
	 */
	public void propagate(PropertyChangeEvent evt) {
		propertyChangeSupport.firePropertyChange(evt);
	}
}
