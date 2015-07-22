package net.canadensys.harvester.occurrence.status;

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
	 * @return list of DwcaResourceStatusModel that require harvest or empty list, never null.
	 */
	public List<DwcaResourceStatusModel> getHarvestRequiredList();

}
