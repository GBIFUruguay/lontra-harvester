package net.canadensys.harvester.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.canadensys.dataportal.occurrence.model.OccurrenceModel;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Test behavior of BulkDataObject serialization
 * @author cgendreau
 *
 */
public class BulkDataObjectTest {
	
	@Test
	public void testBulkDataObjectSerialization(){
		List<String> columns = new ArrayList<String>();
		columns.add("dwcaid");
		columns.add("scientificname");
		
		BulkDataObject<OccurrenceModel> bulkObj = new BulkDataObject<OccurrenceModel>(columns);
		
		OccurrenceModel occModel1 = new OccurrenceModel();
		occModel1.setDwcaid("1");
		occModel1.setScientificname("scientificname 1");
		OccurrenceModel occModel2 = new OccurrenceModel();
		occModel2.setDwcaid("2");
		occModel2.setScientificname("scientificname 2");
		
		bulkObj.addObject(occModel1);
		bulkObj.addObject(occModel2);
		
		//Serialize the Bulk object as JSON
		String jsonRepresentation = null;
		ObjectMapper objMapper = new ObjectMapper();
		try {
			jsonRepresentation = objMapper.writeValueAsString(bulkObj);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			fail();
		}
		
		//Reconstruct a new object based on the JSON representation
		TypeReference<BulkDataObject<OccurrenceModel>> typeRef =  new TypeReference<BulkDataObject<OccurrenceModel>>(){};
		BulkDataObject<OccurrenceModel> reconstructedBulkObj = null;
		try {
			reconstructedBulkObj = objMapper.readValue(jsonRepresentation,typeRef);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Can't reconstruct BulkDataObject from its JSON string");
		}
		
		OccurrenceModel rebuiltObj1 = new OccurrenceModel();
		reconstructedBulkObj.retrieveObject(0, rebuiltObj1);
		assertEquals("1", rebuiltObj1.getDwcaid());
		
		OccurrenceModel rebuiltObj2 = new OccurrenceModel();
		reconstructedBulkObj.retrieveObject(1, rebuiltObj2);
		assertEquals("2", rebuiltObj2.getDwcaid());
	}

}
