package net.canadensys.harvester.occurrence.step;

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.config.ProcessingConfigTest;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

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
public class SynchronousProcessEmlContentStepTest {

	@Autowired
	private ProcessingStepIF synchronousProcessEmlContentStep;

	@Autowired
	@Qualifier(value = "bufferTransactionManager")
	private HibernateTransactionManager txManager;

	@Test
	public void testSynchronousProcessEmlContentStep() {
		Map<SharedParameterEnum, Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH,
				"src/test/resources/dwca-qmor-specimens");
		sharedParameters.put(SharedParameterEnum.SOURCE_FILE_ID,
				"qmor-specimens");
		sharedParameters.put(SharedParameterEnum.RESOURCE_UUID,
				"ada5d0b1-07de-4dc0-83d4-e312f0fb81cb");

		synchronousProcessEmlContentStep.preStep(sharedParameters);
		synchronousProcessEmlContentStep.doStep();
		synchronousProcessEmlContentStep.postStep();

		JdbcTemplate jdbcTemplate = new JdbcTemplate(txManager.getDataSource());
		int count = jdbcTemplate.queryForObject(
				"SELECT count(*) FROM buffer.resource_information",
				BigDecimal.class).intValue();
		assertTrue(count >= 1);

		String alternateIdentifier = jdbcTemplate
				.queryForObject(
						"SELECT alternate_identifier FROM buffer.resource_information where resource_uuid='ada5d0b1-07de-4dc0-83d4-e312f0fb81cb'",
						String.class);
		assertTrue("Collection entomologique Ouellet-Robert (QMOR)"
				.equals(alternateIdentifier));
	}
}
