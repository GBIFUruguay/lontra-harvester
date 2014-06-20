package net.canadensys.harvester.occurrence.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import net.canadensys.dataportal.occurrence.model.OccurrenceModel;
import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;

import org.junit.Test;

/**
 * Test OccurrenceProcessor behavior
 * @author cgendreau
 *
 */
public class OccurrenceProcessorTest {
	
	private static final char NULL_CHAR = '\0';
	
	/**
	 * Test regular processing
	 */
	@Test
	public void testProcessingMechanism(){
		
		OccurrenceProcessor occProcessor = new OccurrenceProcessor();
		
		OccurrenceRawModel rawModel = new OccurrenceRawModel();
		rawModel.setAssociatedmedia("http://www.google.com | http://yahoo.ca");
		rawModel.setCountry("bra");
		//the tab char in the scientific name, the quotes should removed and the NUL should be removed.
		//actually the NUL will be removed by the Mapper but the Processor should also do it
		rawModel.setScientificname("\"Carex \tLinnaeus\""+NULL_CHAR);
		
		rawModel.setDecimallatitude("10.2");
		rawModel.setDecimallongitude("27.3");
		
		rawModel.setStateprovince("Rio de Janeiro");
		rawModel.setEventdate("2011-12-26");
		
		try {
			OccurrenceModel processedModel = ProcessorRunner.runItemProcessor(occProcessor, rawModel, null);
			
			assertEquals("http://www.google.com; http://yahoo.ca", processedModel.getAssociatedmedia());
			assertTrue(processedModel.getHasmedia());
			assertEquals("South America", processedModel.getContinent());
			assertEquals("Rio de Janeiro", processedModel.getStateprovince());
			assertNotNull(processedModel.getDecimallatitude());
			assertNotNull(processedModel.getDecimallongitude());
			assertTrue(processedModel.getHascoordinates());
			
			//scientific name
			assertEquals("Carex", processedModel.getScientificname());
			assertEquals("Linnaeus", processedModel.getScientificnameauthorship());
			
			//decade should be set
			assertEquals(2010, processedModel.getDecade().intValue());
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	/**
	 * Test regular processing with verbatim data.
	 * Verbatimlatitude,Verbatimlongitude, Verbatimeventdate (no date interval)
	 */
	@Test
	public void testVerbatimProcessing(){
		OccurrenceProcessor occProcessor = new OccurrenceProcessor();
		
		OccurrenceRawModel rawModel = new OccurrenceRawModel();
		rawModel.setVerbatimlatitude("10°N");
		rawModel.setVerbatimlongitude("27°W");
		rawModel.setVerbatimeventdate("2011-12-26");
		
		try {
			OccurrenceModel processedModel = occProcessor.process(rawModel, null);
			
			assertNotNull(processedModel.getDecimallatitude());
			assertNotNull(processedModel.getDecimallongitude());
			assertTrue(processedModel.getHascoordinates());
			
			//decade should be set
			assertEquals(2010, processedModel.getDecade().intValue());
			assertEquals(2011,processedModel.getSyear().intValue());
			assertEquals(12,processedModel.getSmonth().intValue());
			assertEquals(26,processedModel.getSday().intValue());
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	/**
	 * Make sure we do not keep an invalid data in processed model.
	 * Decimallatitude, Decimallongitude, Eventdate (not interval)
	 * 
	 */
	@Test
	public void testProcessingBehaviorOnError(){
		OccurrenceProcessor occProcessor = new OccurrenceProcessor();
		
		//we should not keep coordinates if the latitude is wrong but not the longitude
		OccurrenceRawModel rawModel = new OccurrenceRawModel();
		rawModel.setDecimallatitude("10000");
		rawModel.setDecimallongitude("27.3");
		
		rawModel.setEventdate("1904-07-");
		
		try {
			OccurrenceModel processedModel = occProcessor.process(rawModel, null);
			
			assertNull(processedModel.getDecimallatitude());
			assertNull(processedModel.getDecimallongitude());
			assertFalse(processedModel.getHascoordinates());
			
			assertNull(processedModel.getSyear());
			assertNull(processedModel.getSmonth());
			assertNull(processedModel.getSday());
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	/**
	 * Make sure we can parse date interval when provided in Eventdate or Verbatimeventdate.
	 */
	@Test
	public void testDateProcessingBehaviorWhenYDMIsProvided(){
		OccurrenceProcessor occProcessor = new OccurrenceProcessor();
		OccurrenceRawModel rawModel = new OccurrenceRawModel();
		rawModel.setDay("1");
		rawModel.setMonth("10");
		rawModel.setYear("1990");
		
		OccurrenceModel processedModel = occProcessor.process(rawModel, null);
		assertEquals(1990,processedModel.getSyear().intValue());
		assertEquals(10,processedModel.getSmonth().intValue());
		assertEquals(1,processedModel.getSday().intValue());
		
		//partial date
		rawModel = new OccurrenceRawModel();
		rawModel.setMonth("10");
		rawModel.setYear("1990");
		
		processedModel = occProcessor.process(rawModel, null);
		assertEquals(1990,processedModel.getSyear().intValue());
		assertEquals(10,processedModel.getSmonth().intValue());
		assertNull(processedModel.getSday());
		
		//wrong date, we still keep the year
		rawModel = new OccurrenceRawModel();
		rawModel.setDay("1");
		rawModel.setMonth("13");
		rawModel.setYear("1990");
		
		processedModel = occProcessor.process(rawModel, null);
		assertEquals(1990,processedModel.getSyear().intValue());
		assertNull(processedModel.getSmonth());
		assertNull(processedModel.getSday());
		
		//test day,month, year fields precedence
		rawModel = new OccurrenceRawModel();
		rawModel.setDay("1");
		rawModel.setMonth("10");
		rawModel.setYear("1990");
		rawModel.setEventdate("1991-11-2");
		
		processedModel = occProcessor.process(rawModel, null);
		assertEquals(1990,processedModel.getSyear().intValue());
		assertEquals(10,processedModel.getSmonth().intValue());
		assertEquals(1,processedModel.getSday().intValue());
	}
	
	/**
	 * Make sure we can parse date interval when provided in Eventdate or Verbatimeventdate.
	 */
	@Test
	public void testDateIntervalProcessing(){
		OccurrenceProcessor occProcessor = new OccurrenceProcessor();
		List<OccurrenceRawModel> rawModelList = new ArrayList<OccurrenceRawModel>();
		OccurrenceRawModel rawModel = new OccurrenceRawModel();
		rawModel.setEventdate("1904-07-23/1904-07-27");
		rawModelList.add(rawModel);
		
		//make sure we can use verbatimEventDate
		rawModel = new OccurrenceRawModel();
		rawModel.setVerbatimeventdate("1904-07-23/1904-07-27");
		rawModelList.add(rawModel);
		
		//make sure in case we have both, eventDate is used
		rawModel = new OccurrenceRawModel();
		rawModel.setEventdate("1904-07-23/1904-07-27");
		rawModel.setVerbatimeventdate("2004-07-23/2004-07-27");
		rawModelList.add(rawModel);
		
		try {
			OccurrenceModel processedModel;
			for(OccurrenceRawModel curr : rawModelList){
				processedModel = occProcessor.process(curr, null);
				
				assertEquals(1900, processedModel.getDecade().intValue());
				assertEquals(1904,processedModel.getSyear().intValue());
				assertEquals(7,processedModel.getSmonth().intValue());
				assertEquals(23,processedModel.getSday().intValue());
				assertEquals(1904,processedModel.getEyear().intValue());
				assertEquals(7,processedModel.getEmonth().intValue());
				assertEquals(27,processedModel.getEday().intValue());
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	/**
	 * Make sure we do not keep an invalid data in processed model.
	 * Eventdate (interval)
	 * 
	 */
	@Test
	public void testDateIntervalProcessingBehaviorOnError(){
		OccurrenceProcessor occProcessor = new OccurrenceProcessor();
		
		//we should not keep start and end dates if the event date interval is not valid
		OccurrenceRawModel rawModel = new OccurrenceRawModel();
		rawModel.setEventdate("1904-07-27/1904-07-23"); //inverted
		
		try {
			OccurrenceModel processedModel = occProcessor.process(rawModel, null);

			assertNull(processedModel.getSyear());
			assertNull(processedModel.getSmonth());
			assertNull(processedModel.getSday());
			assertNull(processedModel.getEyear());
			assertNull(processedModel.getEmonth());
			assertNull(processedModel.getEday());
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

}
