package net.canadensys.harvester.occurrence.reader;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import net.canadensys.harvester.ItemReaderIF;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.junit.Test;

/**
 * Test the reading of the name(rowType) of all extensions included in a DarwinCore Archive.
 *
 * @author canadensys
 *
 */
public class ExtensionInfoReaderTest {

	@Test
	public void testDwcaExtensionInfoReader() {
		Map<SharedParameterEnum, Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH, "src/test/resources/dwca-qmor-specimens");

		ItemReaderIF<Term> extInfoReader = new DwcaExtensionInfoReader();
		extInfoReader.openReader(sharedParameters);

		Term providedExtension = extInfoReader.read();
		assertEquals(GbifTerm.Multimedia, providedExtension);
	}
}
