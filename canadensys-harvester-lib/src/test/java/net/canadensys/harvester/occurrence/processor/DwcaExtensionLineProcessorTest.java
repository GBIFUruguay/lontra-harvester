package net.canadensys.harvester.occurrence.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceExtensionModel;
import net.canadensys.harvester.ItemProcessorIF;
import net.canadensys.harvester.config.ProcessingConfigTest;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

/**
 * Test DwcaExtensionLineProcessor auto_id assignement
 * 
 * @author cgendreau
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ProcessingConfigTest.class, loader = AnnotationConfigContextLoader.class)
public class DwcaExtensionLineProcessorTest {

	@Autowired
	@Qualifier("extLineProcessor")
	private ItemProcessorIF<OccurrenceExtensionModel, OccurrenceExtensionModel> extLineProcessor;

	@Test
	public void testDwcALineProcessor() {
		Map<SharedParameterEnum, Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.SOURCE_FILE_ID, "MySourceFileId");

		OccurrenceExtensionModel rawModel = new OccurrenceExtensionModel();
		OccurrenceExtensionModel occModel = ProcessorRunner.runItemProcessor(extLineProcessor, rawModel, sharedParameters);

		assertEquals("MySourceFileId", occModel.getSourcefileid());
		assertTrue(occModel.getAuto_id() > 0);
	}
}
