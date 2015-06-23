package net.canadensys.harvester.occurrence.mock;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

public class MockSharedParameters {

	/**
	 * Returns mock shared parameters for the test QMOR resource in
	 * src/test/resources/dwca-qmor-specimens
	 *
	 * @return
	 */
	public static Map<SharedParameterEnum, Object> getQMORSharedParameters() {
		DwcaResourceModel resourceModel = new DwcaResourceModel();
		resourceModel.setId(1);
		resourceModel.setGbif_package_id("ada5d0b1-07de-4dc0-83d4-e312f0fb81cb");
		resourceModel.setSourcefileid("qmor-specimens");

		Map<SharedParameterEnum, Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH, "src/test/resources/dwca-qmor-specimens");
		sharedParameters.put(SharedParameterEnum.RESOURCE_MODEL, resourceModel);
		sharedParameters.put(SharedParameterEnum.RESOURCE_ID, 1);

		return sharedParameters;
	}

	public static DwcaResourceModel getDwcaResourceModel(Integer resourceId, String gbifPackageId, String sourceFileId) {
		DwcaResourceModel resourceModel = new DwcaResourceModel();
		resourceModel.setId(resourceId);
		resourceModel.setGbif_package_id(gbifPackageId);
		resourceModel.setSourcefileid(sourceFileId);

		return resourceModel;
	}

}
