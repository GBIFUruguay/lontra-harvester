package net.canadensys.harvester.occurrence.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Test that JobUId equality and the behavior on JSON serialization.
 * @author canadensys
 *
 */
public class JobUIdTest {

	@Test
	public void testEqualsOverJSONSerialization(){
		ObjectMapper om = new ObjectMapper();
		JobUId jobUId = new JobUId();
		
		try {
			String jsonJobUId = om.writeValueAsString(jobUId);
			JobUId jobUIdFromJson = om.readValue(jsonJobUId, JobUId.class);
			
			assertEquals(jobUId.getUID(), jobUIdFromJson.getUID());
			assertEquals(jobUId, jobUIdFromJson);
			
			//add sourceSourceFileId
			jobUId = new JobUId("MySourceFileId");
			jsonJobUId = om.writeValueAsString(jobUId);
			jobUIdFromJson = om.readValue(jsonJobUId, JobUId.class);
			
			assertEquals(jobUId.getSourceFileId(), jobUIdFromJson.getSourceFileId());
			assertEquals(jobUId, jobUIdFromJson);
			
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			fail();
		} catch (IOException e) {
			fail();
		}
	}
	
	@Test
	public void testEquals(){
		JobUId jobUId1 = new JobUId("MySourceFileId");
		JobUId jobUId2 = new JobUId("MySourceFileId");
		
		//2 objects should NOT be equal even if they use the same sourceFileId
		assertFalse(jobUId1.equals(jobUId2));
	}
	
	/**
	 * Ensure we can use it as a key in a Map.
	 */
	@Test
	public void testAsMapKey(){
		ObjectMapper om = new ObjectMapper();
		JobUId jobUId = new JobUId();
		Map<JobUId,String> testMapKey = new HashMap<JobUId,String>();
		testMapKey.put(jobUId, "testValue");
		
		try {
			String jsonJobUId = om.writeValueAsString(jobUId);
			JobUId jobUIdFromJson = om.readValue(jsonJobUId, JobUId.class);
			
			assertTrue(testMapKey.containsKey(jobUId));
			assertTrue(testMapKey.containsKey(jobUIdFromJson));
			
			//Ensure we can retrieve the value using a deserialized object
			assertEquals("testValue",testMapKey.get(jobUIdFromJson));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			fail();
		} catch (IOException e) {
			fail();
		}
	}
}
