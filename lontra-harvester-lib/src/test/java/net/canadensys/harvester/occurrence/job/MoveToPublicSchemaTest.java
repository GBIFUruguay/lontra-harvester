package net.canadensys.harvester.occurrence.job;

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;

import javax.sql.DataSource;

import net.canadensys.harvester.TestDataHelper;
import net.canadensys.harvester.config.ProcessingConfigTest;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.model.JobStatusModel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

/**
 * Test coverage :
 * -Move to public schema
 * -Log the import
 *
 * @author canadensys
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ProcessingConfigTest.class, loader = AnnotationConfigContextLoader.class)
public class MoveToPublicSchemaTest {

	@Autowired
	private ApplicationContext appContext;

	@Autowired
	private MoveToPublicSchemaJob moveJob;

	private JdbcTemplate jdbcTemplate;

	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Before
	public void setupTest() {

		TestDataHelper.loadTestData(appContext, jdbcTemplate);

		jdbcTemplate.batchUpdate(new String[] {
				"DELETE FROM buffer.occurrence", "DELETE FROM occurrence",
				"DELETE FROM buffer.occurrence_raw",
				"DELETE FROM occurrence_raw",
				"DELETE FROM buffer.contact", "DELETE FROM contact",
				"DELETE FROM buffer.resource_metadata", "DELETE FROM resource_metadata",
				"INSERT INTO buffer.occurrence (auto_id,dwca_id,stateprovince,sourcefileid,resource_id) VALUES (1,'1','Delaware','qmor-specimens',1)",
				"INSERT INTO buffer.occurrence (auto_id,dwca_id,stateprovince,sourcefileid,resource_id) VALUES (2,'3','Florida','qmor-specimens',1)", });
	}

	@After
	public void cleanup() {
		jdbcTemplate.batchUpdate(new String[] {
				"DELETE FROM buffer.occurrence",
				"DELETE FROM occurrence",
				"DELETE FROM buffer.occurrence_raw",
				"DELETE FROM occurrence_raw",
				"DELETE FROM buffer.contact",
				"DELETE FROM contact",
				"DELETE FROM buffer.resource_metadata",
		"DELETE FROM resource_metadata" });
	}

	@Test
	public void testMoveToPublicSchema() {
		JobStatusModel jobStatusModel = new JobStatusModel();

		moveJob.addToSharedParameters(SharedParameterEnum.RESOURCE_ID, 1);
		moveJob.addToSharedParameters(SharedParameterEnum.RESOURCE_NAME, "resource");
		moveJob.addToSharedParameters(SharedParameterEnum.PUBLISHER_NAME, "publisher");
		moveJob.doJob(jobStatusModel);

		String state = jdbcTemplate.queryForObject("SELECT stateprovince FROM occurrence where dwca_id='3'", String.class);
		assertTrue("Florida".equals(state));

		String source = jdbcTemplate.queryForObject("SELECT sourcefileid FROM occurrence where dwca_id='1'", String.class);
		assertTrue("qmor-specimens".equals(source));

		int count = jdbcTemplate.queryForObject("SELECT count(*) FROM occurrence", BigDecimal.class).intValue();
		assertTrue(new Integer(2).equals(count));

		// validate import log
		Integer record_quantity_log = jdbcTemplate.queryForObject("SELECT record_quantity FROM import_log where sourcefileid = 'qmor-specimens' "
				+ "AND id = (SELECT MAX(id) FROM import_log)", Integer.class);
		assertTrue(new Integer(2).equals(record_quantity_log));
	}
}
