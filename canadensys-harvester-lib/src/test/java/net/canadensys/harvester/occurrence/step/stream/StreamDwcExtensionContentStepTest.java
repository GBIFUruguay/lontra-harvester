package net.canadensys.harvester.occurrence.step.stream;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceExtensionModel;
import net.canadensys.harvester.message.ProcessingMessageIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.mapper.OccurrenceExtensionMapper;
import net.canadensys.harvester.occurrence.message.DefaultMessage;
import net.canadensys.harvester.occurrence.mock.writer.MockMessageWriter;
import net.canadensys.harvester.occurrence.processor.DwcaExtensionLineProcessor;
import net.canadensys.harvester.occurrence.reader.DwcaExtensionReader;

import org.junit.Test;

/**
 * Test StreamDwcExtensionContentStep
 * @author cgendreau
 *
 */
public class StreamDwcExtensionContentStepTest {
	
	@Test
	public void testStreamDwcExtensionContentStep(){
		StreamDwcExtensionContentStep streamExtStep = new StreamDwcExtensionContentStep();
		
		MockMessageWriter<ProcessingMessageIF> mockMessageWriter = new MockMessageWriter<ProcessingMessageIF>();
		DwcaExtensionReader<OccurrenceExtensionModel> extReader = new DwcaExtensionReader<OccurrenceExtensionModel>();
		extReader.setMapper(new OccurrenceExtensionMapper());
		
		//create a mock writer
		streamExtStep.setWriter(mockMessageWriter);
		streamExtStep.setReader(extReader);
		streamExtStep.setDwcaLineProcessor(new DwcaExtensionLineProcessor());
		
		Map<SharedParameterEnum,Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH,"src/test/resources/dwca-vascan-checklist");
		sharedParameters.put(SharedParameterEnum.DWCA_EXTENSION_TYPE,"description");
		
		streamExtStep.preStep(sharedParameters);
		streamExtStep.doStep();
		streamExtStep.postStep();
		
		//The list of object that would have been written to the database
		List<ProcessingMessageIF> objList = mockMessageWriter.getContent();
		DefaultMessage firstObj = (DefaultMessage)objList.get(0);
		
		assertEquals(OccurrenceExtensionModel.class, firstObj.getContent().getClass());
	}

}
