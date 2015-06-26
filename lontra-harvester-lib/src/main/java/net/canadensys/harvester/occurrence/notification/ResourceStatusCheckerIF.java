package net.canadensys.harvester.occurrence.notification;

import java.util.List;

import net.canadensys.harvester.occurrence.model.DwcaResourceStatusModel;

/**
 * ResourceStatusChecker is used to check and validate the status of a set of resources.
 *
 * @author cgendreau
 *
 */
public interface ResourceStatusCheckerIF {

	/**
	 * Get all resources that require a harvest.
	 *
	 * @return
	 */
	public List<DwcaResourceStatusModel> getHarvestRequiredList();

}
