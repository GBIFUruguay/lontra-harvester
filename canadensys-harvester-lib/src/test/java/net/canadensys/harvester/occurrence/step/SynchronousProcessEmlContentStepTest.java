package net.canadensys.harvester.occurrence.step;

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.config.ProcessingConfigTest;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ProcessingConfigTest.class, loader = AnnotationConfigContextLoader.class)
public class SynchronousProcessEmlContentStepTest {

	@Autowired
	private ProcessingStepIF synchronousProcessEmlContentStep;

	private JdbcTemplate jdbcTemplate;

	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Before
	public void setupTest() {
		jdbcTemplate.batchUpdate(new String[] { "DELETE FROM buffer.contact", "DELETE FROM buffer.resource_metadata" });
	}

	@Test
	public void testSynchronousProcessEmlContentStep() {
		Map<SharedParameterEnum, Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH, "src/test/resources/dwca-qmor-specimens");
		sharedParameters.put(SharedParameterEnum.SOURCE_FILE_ID, "qmor-specimens");
		sharedParameters.put(SharedParameterEnum.RESOURCE_UUID, "ada5d0b1-07de-4dc0-83d4-e312f0fb81cb");

		synchronousProcessEmlContentStep.preStep(sharedParameters);
		synchronousProcessEmlContentStep.doStep();
		synchronousProcessEmlContentStep.postStep();

		int count = jdbcTemplate.queryForObject("SELECT count(*) FROM buffer.resource_metadata", BigDecimal.class).intValue();
		assertTrue(count >= 1);

		String alternateIdentifier = jdbcTemplate.queryForObject(
				"SELECT alternate_identifier FROM buffer.resource_metadata where resource_uuid='ada5d0b1-07de-4dc0-83d4-e312f0fb81cb'",
				String.class);
		assertTrue("Collection entomologique Ouellet-Robert (QMOR)".equals(alternateIdentifier));
		
		// Test if the foreign key is being set:
		Integer fkey  = jdbcTemplate.queryForObject(
				"SELECT resource_metadata_fkey FROM buffer.contact where contact_type='contact'",
				Integer.class);
		Integer auto_id = jdbcTemplate.queryForObject("SELECT dwca_resource_id FROM buffer.resource_metadata where resource_uuid='ada5d0b1-07de-4dc0-83d4-e312f0fb81cb'", Integer.class);
		assertTrue(fkey==auto_id);		
	}
}
