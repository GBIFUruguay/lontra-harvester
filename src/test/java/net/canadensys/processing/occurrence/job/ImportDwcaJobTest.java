package net.canadensys.processing.occurrence.job;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.config.ProcessingConfigTest;
import net.canadensys.processing.jms.JMSConsumer;
import net.canadensys.processing.jms.JMSConsumerMessageHandler;
import net.canadensys.processing.occurrence.SharedParameterEnum;

import org.hibernate.SessionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.util.concurrent.FutureCallback;

/**
 * Test coverage : 
 * -Read a DarwinCore archive from a folder
 * -Send the content as JMS messages
 * -Insert raw data in buffer schema
 * -Process data and insert results in buffer schema
 * -Wait for completion
 * -Move to public schema
 * -Log the import
 * 
 * Not cover by this test :
 * -GetResourceInfoTask
 * -ComputeGISDataTask
 * @author canadensys
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=ProcessingConfigTest.class, loader=AnnotationConfigContextLoader.class)
public class ImportDwcaJobTest implements FutureCallback<Void>{
	
	private static final String TEST_BROKER_URL = "vm://localhost?broker.persistent=false";
	private static AtomicBoolean jobComplete = new AtomicBoolean(false);
	
	@Autowired
	@Qualifier(value="bufferSessionFactory")
	private SessionFactory sessionFactory;
	
	@Autowired
	private ImportDwcaJob importDwcaJob;
	
	@Autowired
	@Qualifier("insertRawOccurrenceStep")
	private JMSConsumerMessageHandler insertRawOccurrenceStep;
	
	@Autowired
	@Qualifier("processInsertOccurrenceStep")
	private JMSConsumerMessageHandler processInsertOccurrenceStep;
	
	@Autowired
	@Qualifier("insertResourceContactStep")
	private JMSConsumerMessageHandler insertResourceContactStep;
	
    private JdbcTemplate jdbcTemplate;
    
    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }
	
	@Test
	public void testImport(){
				
		//add a local consumer to test the entire loop
		setupTestConsumer();
						
		importDwcaJob.addToSharedParameters(SharedParameterEnum.DWCA_PATH, "src/test/resources/dwca-qmor-specimens");
		importDwcaJob.addToSharedParameters(SharedParameterEnum.DATASET_SHORTNAME, "qmor-specimens");
		
		importDwcaJob.doJob(this);
		synchronized (jobComplete) {
			try {
				jobComplete.wait();
				//validate content of the database
				if(jobComplete.get()){
					
					String state = jdbcTemplate.queryForObject("SELECT stateprovince FROM buffer.occurrence where dwcaid='3'", String.class);
					assertTrue("Florida".equals(state));
					
					String source = jdbcTemplate.queryForObject("SELECT sourcefileid FROM buffer.occurrence where dwcaid='1'", String.class);
					assertTrue("qmor-specimens".equals(source));
					
					String resource_contact = jdbcTemplate.queryForObject("SELECT name FROM buffer.resource_contact where dataset_shortname='qmor-specimens'", String.class);
					assertTrue("Louise Cloutier".equals(resource_contact));
					
					int count = jdbcTemplate.queryForObject("SELECT count(*) FROM buffer.occurrence",BigDecimal.class).intValue();
					assertTrue(new Integer(11).equals(count));
				}
				else{
					fail();
				}
			} catch (InterruptedException e) {
				fail();
			}
		}
	}
	
	/**
	 * This consumer will write to the database specified by the sessionFactory bean
	 */
	private void setupTestConsumer(){
		JMSConsumer reader = new JMSConsumer(TEST_BROKER_URL);
		reader.registerHandler(insertRawOccurrenceStep);
		reader.registerHandler(processInsertOccurrenceStep);
		reader.registerHandler(insertResourceContactStep);
		
		try {
			((ProcessingStepIF)insertRawOccurrenceStep).preStep(null);
			((ProcessingStepIF)processInsertOccurrenceStep).preStep(null);
			((ProcessingStepIF)insertResourceContactStep).preStep(null);
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		reader.open();
	}
	
	@Override
	public void onSuccess(Void arg0) {
		synchronized (jobComplete) {
			jobComplete.set(true);
			jobComplete.notifyAll();
		}
	}
	@Override
	public void onFailure(Throwable arg0) {
		fail();
	}
}
