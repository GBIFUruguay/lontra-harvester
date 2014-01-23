package net.canadensys.harvester.occurrence.job;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicBoolean;

import net.canadensys.harvester.ProcessingStepIF;
import net.canadensys.harvester.config.ProcessingConfigTest;
import net.canadensys.harvester.jms.JMSConsumer;
import net.canadensys.harvester.jms.JMSConsumerMessageHandlerIF;
import net.canadensys.harvester.jms.control.JMSControlConsumer;
import net.canadensys.harvester.jms.control.JMSControlConsumerMessageHandlerIF;
import net.canadensys.harvester.message.ControlMessageIF;
import net.canadensys.harvester.message.control.NodeErrorControlMessage;
import net.canadensys.harvester.occurrence.SharedParameterEnum;

import org.hibernate.SessionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
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
	private static AtomicBoolean controlMessageReceived = new AtomicBoolean(false);
	
	private static final int EXPECTED_NUMBER_OF_RESULTS = 11;
	private static final int MAX_NUMBER_OF_ATTEMP = 5;
	
	@Autowired
	@Qualifier(value="bufferSessionFactory")
	private SessionFactory sessionFactory;
	
	@Autowired
	@Qualifier(value="publicTransactionManager")
	private HibernateTransactionManager txManager;
	
	private JMSConsumer reader;
	private JMSControlConsumer controlConsumer;
	
	@Autowired
	private ImportDwcaJob importDwcaJob;
	
	@Autowired
	@Qualifier("insertRawOccurrenceStep")
	private JMSConsumerMessageHandlerIF insertRawOccurrenceStep;
	
	@Autowired
	@Qualifier("processInsertOccurrenceStep")
	private JMSConsumerMessageHandlerIF processInsertOccurrenceStep;
	
	@Autowired
	@Qualifier("insertResourceContactStep")
	private JMSConsumerMessageHandlerIF insertResourceContactStep;
	
	@Test
	public void testImport(){
		JdbcTemplate jdbcTemplate = new JdbcTemplate(txManager.getDataSource());
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
					int count = jdbcTemplate.queryForObject("SELECT count(*) FROM buffer.occurrence", BigDecimal.class).intValue();
					//give a chance to the database to be updated (since it's triggered by a JMS message)
					int nbOfAttemp = 0;
					while(count != EXPECTED_NUMBER_OF_RESULTS && nbOfAttemp < MAX_NUMBER_OF_ATTEMP){
						nbOfAttemp++;
						Thread.sleep(1000);
						count = jdbcTemplate.queryForObject("SELECT count(*) FROM buffer.occurrence", BigDecimal.class).intValue();
					}
					
					String state = jdbcTemplate.queryForObject("SELECT stateprovince FROM buffer.occurrence where dwcaid='3'", String.class);
					assertTrue("Florida".equals(state));
					
					String source = jdbcTemplate.queryForObject("SELECT sourcefileid FROM buffer.occurrence where dwcaid='1'", String.class);
					assertTrue("qmor-specimens".equals(source));
					
					String resource_contact = jdbcTemplate.queryForObject("SELECT name FROM buffer.resource_contact where dataset_shortname='qmor-specimens'", String.class);
					assertTrue("Louise Cloutier".equals(resource_contact));

					assertTrue(new Integer(EXPECTED_NUMBER_OF_RESULTS).equals(count));
				}
				else{
					fail();
				}
			} catch (InterruptedException e) {
				fail();
			}
			finally{
				reader.close();
				controlConsumer.close();
			}
		}
	}
	
	/**
	 * Test the behavior of a failing import.
	 * A common reason for failing is when data can not fit into the defined space in the database.
	 */
	@Test
	public void testFailedImport(){
		
		//add a local consumer to test the entire loop
		setupTestConsumer();
						
		importDwcaJob.addToSharedParameters(SharedParameterEnum.DWCA_PATH, "src/test/resources/dwca-qmor-specimens-broken");
		importDwcaJob.addToSharedParameters(SharedParameterEnum.DATASET_SHORTNAME, "qmor-specimens");
		
		importDwcaJob.doJob(this);
		synchronized (controlMessageReceived) {
			try {
				controlMessageReceived.wait();
				//validate content of the database
				if(!controlMessageReceived.get()){
					fail();
				}
			} catch (InterruptedException e) {
				fail();
			}
			finally{
				reader.close();
				controlConsumer.close();
			}
		}
		
	}
	
	/**
	 * This consumer will write to the database specified by the sessionFactory bean
	 */
	private void setupTestConsumer(){
		reader = new JMSConsumer(TEST_BROKER_URL);
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
		
		controlConsumer = new JMSControlConsumer(TEST_BROKER_URL);
		controlConsumer.registerHandler(new MockControlMessageHandler());
		controlConsumer.open();
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
		synchronized (jobComplete) {
			jobComplete.set(false);
			jobComplete.notifyAll();
		}
	}
	
	private class MockControlMessageHandler implements JMSControlConsumerMessageHandlerIF, FutureCallback<Void>{

		@Override
		public Class<?> getMessageClass() {
			return NodeErrorControlMessage.class;
		}

		@Override
		public boolean handleMessage(ControlMessageIF message) {
			synchronized (controlMessageReceived) {
				System.out.println(((NodeErrorControlMessage)message).getErrorMessage());
				controlMessageReceived.set(true);
				controlMessageReceived.notifyAll();
			}
			return true;
		}

		@Override
		public void onSuccess(Void result) {
			//fail();
		}

		@Override
		public void onFailure(Throwable t) {
			//nothing, we handle that case through the ErrorControl Message.
		}
	}
}
