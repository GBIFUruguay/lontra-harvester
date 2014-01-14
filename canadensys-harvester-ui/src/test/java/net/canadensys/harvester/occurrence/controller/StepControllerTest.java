package net.canadensys.harvester.occurrence.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import net.canadensys.harvester.config.TestConfig;
import net.canadensys.harvester.occurrence.model.ResourceModel;

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

		ResourceModel resourceModel = new ResourceModel();

		resourceModel.setName("Test resource");
		resourceModel.setArchive_url("http://locahost/testname.zip");
		resourceModel.setKey("kagf93u");
		resourceModel.setSource_file_id("test-resource");

		// save it
		stepController.updateResourceModel(resourceModel);

		List<ResourceModel> resourceModelList = stepController
				.getResourceModelList();
		boolean found = false;
		for (ResourceModel currModel : resourceModelList) {
			if ("test-resource".equals(currModel.getSource_file_id())) {
				found = true;
				assertEquals("kagf93u", currModel.getKey());
			}
		}
		assertTrue(found);
	}
}
