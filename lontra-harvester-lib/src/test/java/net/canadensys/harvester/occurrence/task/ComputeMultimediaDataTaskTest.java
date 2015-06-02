package net.canadensys.harvester.occurrence.task;

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Map;

import net.canadensys.harvester.ItemTaskIF;
import net.canadensys.harvester.config.ProcessingConfigTest;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.mock.MockSharedParameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

/**
 * Test ComputeMultimediaDataTask built from XML configuration file.
 *
 * @author cgendreau
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ProcessingConfigTest.class, loader = AnnotationConfigContextLoader.class)
public class ComputeMultimediaDataTaskTest {

	@Autowired
	private ItemTaskIF computeMultimediaDataTask;

	@Autowired
	@Qualifier(value = "bufferTransactionManager")
	private HibernateTransactionManager txManager;

	@Test
	public void testComputeMultimediaData() {
		JdbcTemplate jdbcTemplate = new JdbcTemplate(txManager.getDataSource());
		jdbcTemplate.update("DELETE FROM buffer.occurrence_extension");
		jdbcTemplate.update("DELETE FROM buffer.occurrence");

		Map<SharedParameterEnum, Object> sharedParameters = MockSharedParameters.getQMORSharedParameters();

		// Insert mock data
		jdbcTemplate.update("INSERT INTO buffer.occurrence (auto_id, dwca_id,resource_id,sourcefileid,hasmedia) VALUES (?,?,?,?,?)",
				1, "1", sharedParameters.get(SharedParameterEnum.RESOURCE_ID), sharedParameters.get(SharedParameterEnum.SOURCE_FILE_ID), false);

		jdbcTemplate.update("INSERT INTO buffer.occurrence_extension (auto_id, dwca_id,resource_id,sourcefileid,ext_type) VALUES (?,?,?,?,?)",
				1, "1", sharedParameters.get(SharedParameterEnum.RESOURCE_ID), sharedParameters.get(SharedParameterEnum.SOURCE_FILE_ID),
				"Multimedia");

		computeMultimediaDataTask.execute(sharedParameters);

		int count = jdbcTemplate.queryForObject("SELECT count(*) FROM buffer.occurrence where hasmedia = true AND auto_id = 1", BigDecimal.class)
				.intValue();
		assertTrue(count == 1);
	}

}
