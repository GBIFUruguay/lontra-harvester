package net.canadensys.harvester.occurrence.step;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Map;

import net.canadensys.harvester.StepIF;
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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ProcessingConfigTest.class, loader = AnnotationConfigContextLoader.class)
public class SynchronousProcessOccurrenceExtensionStepTest {

	@Autowired
	private StepIF synchronousProcessOccurrenceExtensionStep;

	@Autowired
	@Qualifier(value = "bufferTransactionManager")
	private HibernateTransactionManager txManager;

	@Test
	public void testSynchronousProcessOccurrenceExtensionStep() {
		JdbcTemplate jdbcTemplate = new JdbcTemplate(txManager.getDataSource());
		jdbcTemplate.update("DELETE FROM buffer.occurrence_extension");

		Map<SharedParameterEnum, Object> sharedParameters = MockSharedParameters.getQMORSharedParameters();

		synchronousProcessOccurrenceExtensionStep.preStep(sharedParameters);
		synchronousProcessOccurrenceExtensionStep.doStep();
		synchronousProcessOccurrenceExtensionStep.postStep();

		int count = jdbcTemplate.queryForObject("SELECT count(*) FROM buffer.occurrence_extension", BigDecimal.class).intValue();
		assertTrue(count >= 1);

		Map<String, String> data = jdbcTemplate.queryForObject("SELECT ext_data FROM buffer.occurrence_extension WHERE dwcaid='1'", Map.class);
		assertEquals("images/jpeg", data.get("format"));
	}

}
