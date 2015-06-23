package net.canadensys.harvester.occurrence.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import net.canadensys.dataportal.occurrence.model.OccurrenceRawModel;
import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.mock.MockSharedParameters;

import org.junit.Test;

/**
 * Test the reading of a DarwinCore Archive file and get a object back.
 * Ensure the default values are read
 *
 * @author canadensys
 *
 */
public class DwcaReaderTest {

	@Test
	public void testDwcaItemReader() {
		Map<SharedParameterEnum, Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH, "src/test/resources/dwca-qmor-specimens");
		sharedParameters.put(SharedParameterEnum.RESOURCE_MODEL,
				MockSharedParameters.getDwcaResourceModel(1, UUID.randomUUID().toString(), "qmor-specimens"));
		int count = 0;

		ItemReaderIF<OccurrenceRawModel> dwcaItemReader = new DwcaItemReader();
		dwcaItemReader.openReader(sharedParameters);

		// check headers are correctly set
		@SuppressWarnings("unchecked")
		List<String> usedTerms = (List<String>) sharedParameters.get(SharedParameterEnum.DWCA_USED_TERMS);
		// the reader should not set terms that can not be stored in the model (but a warning will be in the log)
		assertFalse(usedTerms.contains("source"));

		OccurrenceRawModel rawModel = dwcaItemReader.read();
		count++;
		// ensure that we read default values
		assertEquals("PreservedSpecimen", rawModel.getBasisofrecord());
		assertEquals("Rigaud", rawModel.getMunicipality());

		while (dwcaItemReader.read() != null) {
			count++;
		}

		// Ensure we read the entire file
		assertEquals(11, count);
	}

	/**
	 * Test an archive where the id column is also a field (e.g. catalogNumber).
	 */
	@Test
	public void testDwcaItemReaderIdColumn() {
		Map<SharedParameterEnum, Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH, "src/test/resources/dwca-qmor-specimens-id");
		sharedParameters.put(SharedParameterEnum.RESOURCE_MODEL,
				MockSharedParameters.getDwcaResourceModel(1, UUID.randomUUID().toString(), "qmor-specimens"));

		ItemReaderIF<OccurrenceRawModel> dwcaItemReader = new DwcaItemReader();
		dwcaItemReader.openReader(sharedParameters);

		OccurrenceRawModel rawModel = dwcaItemReader.read();
		assertEquals("1", rawModel.getCatalognumber());
		assertEquals("Gomphus", rawModel.getGenus());
	}

	@Test
	public void testDwcaItemReaderAbort() {
		Map<SharedParameterEnum, Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH, "src/test/resources/dwca-qmor-specimens");
		sharedParameters.put(SharedParameterEnum.RESOURCE_MODEL,
				MockSharedParameters.getDwcaResourceModel(1, UUID.randomUUID().toString(), "qmor-specimens"));

		ItemReaderIF<OccurrenceRawModel> dwcaItemReader = new DwcaItemReader();
		dwcaItemReader.openReader(sharedParameters);

		OccurrenceRawModel rawModel = dwcaItemReader.read();
		assertNotNull(rawModel);
		// cancel the reader
		dwcaItemReader.abort();
		rawModel = dwcaItemReader.read();
		assertNull(rawModel);
	}
}
