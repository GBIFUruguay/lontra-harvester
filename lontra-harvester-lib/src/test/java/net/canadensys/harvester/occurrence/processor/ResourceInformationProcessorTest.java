package net.canadensys.harvester.occurrence.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.UUID;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.dataportal.occurrence.model.ResourceMetadataModel;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.mock.MockSharedParameters;

import org.gbif.metadata.eml.Eml;
import org.junit.Test;

/**
 * Test ResourceInformationProcessor behavior.
 *
 * @author cgendreau
 *
 */
public class ResourceInformationProcessorTest {

	/**
	 * Test regular processing
	 */
	@Test
	public void testProcessingMechanism() {
		Map<SharedParameterEnum, Object> sharedParameters = MockSharedParameters.getQMORSharedParameters();
		DwcaResourceModel resourceModel = (DwcaResourceModel) sharedParameters.get(SharedParameterEnum.RESOURCE_MODEL);

		String uuid = UUID.randomUUID().toString();
		resourceModel.setGbif_package_id(uuid);

		ResourceMetadataProcessor informationProcessor = new ResourceMetadataProcessor();
		Eml eml = new Eml();

		eml.setAbstract("This is a generic abstract information");
		eml.setTitle("This is a generic title");
		eml.setGuid(uuid);
		try {
			ResourceMetadataModel processedInformation = informationProcessor.process(eml, sharedParameters);
			assertEquals("This is a generic abstract information", processedInformation.get_abstract());
			assertEquals("This is a generic title", processedInformation.getTitle());
			assertEquals(uuid, processedInformation.getGbif_package_id());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
}
