package net.canadensys.harvester.occurrence.notification;

import java.util.List;

import net.canadensys.dataportal.occurrence.model.ResourceModel;

/**
 * ResourceStatusNotifier is used to check and validate the status of a set of resources.
 * @author cgendreau
 *
 */
public interface ResourceStatusNotifierIF {
	
	/**
	 * Get all resources that require a harvest.
	 * @return
	 */
	public List<ResourceModel> getHarvestRequiredList();

}
