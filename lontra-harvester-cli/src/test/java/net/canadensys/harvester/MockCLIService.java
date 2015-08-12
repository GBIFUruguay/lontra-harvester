package net.canadensys.harvester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.mock.MockResource;
import net.canadensys.harvester.model.CliOption;

/**
 * Mock implementation for testing purpose.
 * 
 * @author cgendreau
 *
 */
public class MockCLIService implements CLIService {

	@Override
	public List<DwcaResourceModel> getResourceList() {
		DwcaResourceModel mockDwcaResourceModel = MockResource.getMockQMORResource();
		List<DwcaResourceModel> resourceList = new ArrayList<DwcaResourceModel>();
		resourceList.add(mockDwcaResourceModel);
		return resourceList;
	}

	@Override
	public DwcaResourceModel loadResourceModel(String resourceIdentifier) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void importDwca(DwcaResourceModel resourceModel, CliOption cliOption) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void moveToPublicSchema(DwcaResourceModel resourceModel) {
		// TODO Auto-generated method stub

	}

	@Override
	public void computeUniqueValueJob() {
		// TODO Auto-generated method stub

	}

}
