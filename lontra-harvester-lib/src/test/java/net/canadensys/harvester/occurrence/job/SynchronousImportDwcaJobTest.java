package net.canadensys.harvester.occurrence.job;

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;

import javax.sql.DataSource;

import net.canadensys.harvester.StepIF;
import net.canadensys.harvester.config.ProcessingConfigTest;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.step.SynchronousProcessEmlContentStep;
import net.canadensys.harvester.occurrence.step.SynchronousProcessOccurrenceStep;

import org.hibernate.SessionFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

/**
 * Test coverage : -Read a DarwinCore archive from a folder -Insert raw data in
 * buffer schema -Process data and insert results in buffer schema
 * 
 * @author canadensys
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = SynchronousImportDwcaJobTest.SynchronousProcessingConfigTest.class, loader = AnnotationConfigContextLoader.class)
public class SynchronousImportDwcaJobTest {

	private static final int EXPECTED_NUMBER_OF_RESULTS = 11;

	@Autowired
	@Qualifier(value = "bufferSessionFactory")
	private SessionFactory sessionFactory;

	private JdbcTemplate jdbcTemplate;

	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Autowired
	private ImportDwcaJob importDwcaJob;

	@Before
	public void setupTest() {
		jdbcTemplate.batchUpdate(new String[] { "DELETE FROM buffer.contact", "DELETE FROM buffer.resource_metadata" });
	}

	@Configuration
	@Import(ProcessingConfigTest.class)
	public static class SynchronousProcessingConfigTest extends ProcessingConfigTest {

		@Override
		@Bean(name = "streamDwcContentStep")
		public StepIF streamDwcContentStep() {
			return new SynchronousProcessOccurrenceStep();
		}

		@Override
		@Bean(name = "streamEmlContentStep")
		public StepIF streamEmlContentStep() {
			return new SynchronousProcessEmlContentStep();
		}
	}

	@Test
	public void testImport() {
		importDwcaJob.addToSharedParameters(SharedParameterEnum.DWCA_PATH, "src/test/resources/dwca-qmor-specimens");
		importDwcaJob.addToSharedParameters(SharedParameterEnum.SOURCE_FILE_ID, "qmor-specimens");
		importDwcaJob.addToSharedParameters(SharedParameterEnum.RESOURCE_UUID, "ada5d0b1-07de-4dc0-83d4-e312f0fb81cb");
		importDwcaJob.addToSharedParameters(SharedParameterEnum.RESOURCE_ID, 1);

		JobStatusModel jobStatusModel = new JobStatusModel();
		importDwcaJob.doJob(jobStatusModel);

		int count = jdbcTemplate.queryForObject("SELECT count(*) FROM buffer.occurrence", BigDecimal.class).intValue();

		String state = jdbcTemplate.queryForObject("SELECT stateprovince FROM buffer.occurrence where dwcaid='3'", String.class);
		assertTrue("Florida".equals(state));

		String source = jdbcTemplate.queryForObject("SELECT sourcefileid FROM buffer.occurrence where dwcaid='1'", String.class);
		assertTrue("qmor-specimens".equals(source));

		// Test information is being also processed from EML content:
		String alternateIdentifier = jdbcTemplate.queryForObject(
				"SELECT alternate_identifier FROM buffer.resource_metadata where resource_uuid='ada5d0b1-07de-4dc0-83d4-e312f0fb81cb'", String.class);
		assertTrue("Collection entomologique Ouellet-Robert (QMOR)".equals(alternateIdentifier));

		assertTrue(new Integer(EXPECTED_NUMBER_OF_RESULTS).equals(count));
	}

}
