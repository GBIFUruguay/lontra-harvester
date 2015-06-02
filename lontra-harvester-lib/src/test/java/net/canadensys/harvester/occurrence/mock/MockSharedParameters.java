package net.canadensys.harvester.occurrence.mock;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.harvester.occurrence.SharedParameterEnum;

public class MockSharedParameters {

	/**
	 * Returns mock shared parameters for the test QMOR resource in
	 * src/test/resources/dwca-qmor-specimens
	 *
	 * @return
	 */
	public static Map<SharedParameterEnum, Object> getQMORSharedParameters() {
		Map<SharedParameterEnum, Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH, "src/test/resources/dwca-qmor-specimens");
		sharedParameters.put(SharedParameterEnum.SOURCE_FILE_ID, "qmor-specimens");
		sharedParameters.put(SharedParameterEnum.GBIF_PACKAGE_ID, "ada5d0b1-07de-4dc0-83d4-e312f0fb81cb");
		sharedParameters.put(SharedParameterEnum.RESOURCE_ID, 1);

		return sharedParameters;
	}

}
