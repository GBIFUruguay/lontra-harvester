package net.canadensys.harvester.occurrence.reader;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.mock.MockHabitObject;
import net.canadensys.harvester.occurrence.mock.mapper.MockHabitMapper;

import org.junit.Test;

/**
 * Test the reading of a DarwinCore extension file and get a custom object back.
 * @author canadensys
 *
 */
public class DwcaExtensionReaderTest {
	
	@Test
	public void testExtensionReading(){
		Map<SharedParameterEnum,Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH,"src/test/resources/dwca-vascan-checklist");
		sharedParameters.put(SharedParameterEnum.DWCA_EXTENSION_TYPE,"description");
		
		DwcaExtensionReader<MockHabitObject> extReader = new DwcaExtensionReader<MockHabitObject>();
		extReader.setMapper(new MockHabitMapper());
		
		extReader.openReader(sharedParameters);
		MockHabitObject obj = extReader.read();
		assertEquals("herb", obj.getDescription());
		
		//ensure that we read default values
		assertEquals("EN", obj.getLanguage());
	}
}
