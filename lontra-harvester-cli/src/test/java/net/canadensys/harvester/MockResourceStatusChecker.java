package net.canadensys.harvester;

import java.util.List;

import net.canadensys.harvester.occurrence.model.DwcaResourceStatusModel;
import net.canadensys.harvester.occurrence.status.ResourceStatusCheckerIF;

/**
 * Mock implementation for testing purpose.
 * 
 * @author cgendreau
 *
 */
public class MockResourceStatusChecker implements ResourceStatusCheckerIF {

	@Override
	public List<DwcaResourceStatusModel> getHarvestRequiredList() {
		return null;
	}

}
