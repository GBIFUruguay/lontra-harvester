package net.canadensys.harvester.occurrence.status;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.sql.DataSource;

import net.canadensys.dataportal.occurrence.dao.ImportLogDAO;
import net.canadensys.harvester.TestDataHelper;
import net.canadensys.harvester.config.ProcessingConfigTest;
import net.canadensys.harvester.occurrence.model.DwcaResourceStatusModel;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ProcessingConfigTest.class, loader = AnnotationConfigContextLoader.class)
public class ResourceStatusCheckerTest {

	@Autowired
	private ApplicationContext appContext;

	private JdbcTemplate jdbcTemplate;

	@Autowired
	private ResourceStatusCheckerIF resourceStatusChecker;

	@Autowired
	private ImportLogDAO importLogDAO;

	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Before
	public void setupTest() {
		TestDataHelper.loadTestData(appContext, jdbcTemplate);
	}

	/**
	 * Assumptions:
	 * -Date from ipt_rss.xml file is 2014-07-11
	 * -Date from insert-test-data.sql is 2014-07-10
	 */
	@Test
	public void testGetHarvestRequiredList() {
		List<DwcaResourceStatusModel> resourceStatusModelList = resourceStatusChecker.getHarvestRequiredList();

		assertFalse(resourceStatusModelList.isEmpty());
		DwcaResourceStatusModel dwcaResourceStatusModel = resourceStatusModelList.get(0);
		assertNotNull(dwcaResourceStatusModel.getLastHarvestDate());

		// Record mock Import event
		jdbcTemplate.update("INSERT INTO import_log (id,sourcefileid,gbif_package_id,updated_by,event_end_date_time) VALUES " +
				"(2,'qmor-specimens','ada5d0b1-07de-4dc0-83d4-e312f0fb81cb','Jim','2015-06-26')");

		// Ensure we have not harvest required
		resourceStatusModelList = resourceStatusChecker.getHarvestRequiredList();
		assertTrue(resourceStatusModelList.isEmpty());
	}
}
