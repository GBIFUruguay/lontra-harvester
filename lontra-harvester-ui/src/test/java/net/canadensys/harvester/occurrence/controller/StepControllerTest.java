package net.canadensys.harvester.occurrence.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.config.TestConfig;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

/**
 * Main test class StepController.
 * 
 * @author canadensys
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfig.class, loader = AnnotationConfigContextLoader.class)
public class StepControllerTest {

	@Autowired
	private StepControllerIF stepController;

	@Test
	public void testResourceManagement() {

		DwcaResourceModel resourceModel = new DwcaResourceModel();

		resourceModel.setName("Test resource");
		resourceModel.setArchive_url("http://locahost/testname.zip");
		resourceModel.setResource_uuid("kagf93u");
		resourceModel.setSourcefileid("test-resource");

		// save it
		boolean saved = stepController.updateResourceModel(resourceModel);

		List<DwcaResourceModel> resourceModelList = stepController.getResourceModelList();
		boolean found = false;
		for (DwcaResourceModel currModel : resourceModelList) {
			if ("test-resource".equals(currModel.getSourcefileid())) {
				found = true;
				assertEquals("kagf93u", currModel.getResource_uuid());
			}
		}
		assertTrue(found);
	}
}
