package net.canadensys.harvester.occurrence.reader;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.gbif.metadata.eml.Eml;
import org.junit.Test;

/**
 * Test the reading of an EML file and get an Eml object back.
 * @author canadensys
 *
 */
public class DwcaEmlReaderTest {
	
	@Test
	public void testEmlRead(){
		Map<SharedParameterEnum,Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH,"src/test/resources/dwca-qmor-specimens");
		ItemReaderIF<Eml> emlreader = new DwcaEmlReader();
		emlreader.openReader(sharedParameters);
		Eml eml = emlreader.read();
		assertEquals("Louise Cloutier",eml.getContact().getFullName());
	}
}
