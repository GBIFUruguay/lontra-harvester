package net.canadensys.harvester.mock;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;

public class MockResource {

	public static final String QMOR_SOURCEFILE_ID = "qmor-specimens";
	public static final String QMOR_PACKAGE_ID = "ada5d0b1-07de-4dc0-83d4-e312f0fb81cb";

	/**
	 * Returns mock shared parameters for the test QMOR resource in
	 * src/test/resources/dwca-qmor-specimens
	 * 
	 * @return
	 */
	public static DwcaResourceModel getMockQMORResource() {
		DwcaResourceModel resourceModel = new DwcaResourceModel();
		resourceModel.setId(1);
		resourceModel.setGbif_package_id(QMOR_PACKAGE_ID);
		resourceModel.setSourcefileid(QMOR_SOURCEFILE_ID);
		resourceModel.setName("QMOR");
		return resourceModel;
	}

}
