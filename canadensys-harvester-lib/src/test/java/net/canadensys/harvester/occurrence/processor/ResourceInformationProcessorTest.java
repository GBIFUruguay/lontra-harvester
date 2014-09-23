package net.canadensys.harvester.occurrence.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;

import net.canadensys.dataportal.occurrence.model.ResourceInformationModel;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.gbif.metadata.eml.Eml;
import org.junit.Test;

/**
 * Test OccurrenceProcessor behavior
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
		HashMap<SharedParameterEnum, Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		ResourceInformationProcessor informationProcessor = new ResourceInformationProcessor();
		Eml eml = new Eml();
		sharedParameters.put(SharedParameterEnum.RESOURCE_UUID, "foo");
		eml.setAbstract("This is a generic abstract information");
		eml.setTitle("This is a generic title");
		eml.setGuid("foo");
		try {
			ResourceInformationModel processedInformation = informationProcessor.process(eml, sharedParameters);
			assertEquals("This is a generic abstract information", processedInformation.get_abstract());
			assertEquals("This is a generic title", processedInformation.getTitle());
			assertEquals("foo", processedInformation.getResource_uuid());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
}
