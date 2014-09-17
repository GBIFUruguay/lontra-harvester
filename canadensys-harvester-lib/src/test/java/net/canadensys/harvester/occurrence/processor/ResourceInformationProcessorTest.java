package net.canadensys.harvester.occurrence.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import net.canadensys.dataportal.occurrence.model.ResourceInformationModel;

import org.gbif.metadata.eml.Eml;
import org.junit.Test;

/**
 * Test OccurrenceProcessor behavior
 * @author cgendreau
 *
 */
public class ResourceInformationProcessorTest {
	
	/**
	 * Test regular processing
	 */
	@Test
	public void testProcessingMechanism(){
		
		ResourceInformationProcessor informationProcessor = new ResourceInformationProcessor();		
		Eml eml = new Eml();
		eml.setAbstract("This is a generic abstract information");
		eml.setTitle("This is a generic title");
		try {
			ResourceInformationModel processedInformation = informationProcessor.process(eml, null);
			assertEquals("This is a generic abstract information", processedInformation.get_abstract());
			assertEquals("This is a generic title", processedInformation.getTitle());
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
}
