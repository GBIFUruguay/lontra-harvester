package net.canadensys.harvester.mapper;

import static org.junit.Assert.*;

import org.gbif.dwc.terms.DcTerm;
import org.junit.Test;

/**
 * Test TermMapper implementation.
 *
 */
public class TermMapperTest {

	@Test
	public void testTermMapping() {
		assertEquals(DcTerm.license, TermMapper.getInstance().getTermMapping(DcTerm.rights));
		// Test with a Term we know we do not have a mappin for
		assertNull(TermMapper.getInstance().getTermMapping(DcTerm.accrualPeriodicity));
	}
}
