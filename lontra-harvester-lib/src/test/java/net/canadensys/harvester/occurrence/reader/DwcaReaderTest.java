package net.canadensys.harvester.occurrence.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
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

	private final int QMOR_EXPECTED_NUMBER_OF_RECORDS = 11;

	@Test
	public void testDwcaItemReader() {
		Map<SharedParameterEnum, Object> sharedParameters = MockSharedParameters.getQMORSharedParameters();
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
		assertEquals(QMOR_EXPECTED_NUMBER_OF_RECORDS, count);
	}

	@Test
	public void testDwcaItemReaderWithExclusionList() {
		Map<SharedParameterEnum, Object> sharedParameters = MockSharedParameters.getQMORSharedParameters();
		List<String> dwcaIdExclusionList = new ArrayList<String>();
		dwcaIdExclusionList.add("4");
		sharedParameters.put(SharedParameterEnum.DWCA_ID_EXCLUSION_LIST, dwcaIdExclusionList);
		int count = 0;

		ItemReaderIF<OccurrenceRawModel> dwcaItemReader = new DwcaItemReader();
		dwcaItemReader.openReader(sharedParameters);

		OccurrenceRawModel rawModel = dwcaItemReader.read();
		while (rawModel != null) {
			assertFalse("The dwcaid 4 should be skipped.", "4".equals(rawModel.getId()));
			count++;
			rawModel = dwcaItemReader.read();
		}

		// Ensure we read the entire file
		assertEquals(QMOR_EXPECTED_NUMBER_OF_RECORDS - 1, count);
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
		Map<SharedParameterEnum, Object> sharedParameters = MockSharedParameters.getQMORSharedParameters();

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
