package net.canadensys.harvester.occurrence.mapper;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;

import org.junit.Test;

/**
 * Test the mapping from a map of properties to an OccurrenceRawModel instance.
 * @author cgendreau
 *
 */
public class OccurrenceMapperTest {
	
	private static final char NULL_CHAR = '\0';
	
	@Test
	public void testMapping(){
		OccurrenceMapper occMapper = new OccurrenceMapper();
		Map<String,Object> properties = new HashMap<String,Object>();
		
		properties.put("id", "1");
		properties.put("country", "test country"+NULL_CHAR);
		
		OccurrenceRawModel rawModel = occMapper.mapElement(properties);
		
		//make sure the id is transposed to dwcaid field
		assertEquals("1", rawModel.getDwcaid());
		
		//ensure we do not map invalid characters
		assertEquals("test country", rawModel.getCountry());
	}

}
