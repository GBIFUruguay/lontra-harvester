package net.canadensys.processing.occurrence.job;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import net.canadensys.processing.AbstractProcessingJob;
import net.canadensys.processing.ItemMapperIF;
import net.canadensys.processing.ProcessingStepIF;
import net.canadensys.processing.jms.JMSConsumer;
import net.canadensys.processing.jms.JMSConsumerMessageHandler;
import net.canadensys.processing.jms.JMSWriter;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.mock.MockHabitObject;
import net.canadensys.processing.occurrence.mock.mapper.MockHabitMapper;
import net.canadensys.processing.occurrence.mock.writer.MockObjectWriter;
import net.canadensys.processing.occurrence.reader.DwcaExtensionReader;
import net.canadensys.processing.occurrence.step.async.GenericAsyncStep;
import net.canadensys.processing.occurrence.step.stream.GenericStreamStep;

import org.junit.Test;

import com.google.common.util.concurrent.FutureCallback;

/**
 * Test aiming to test a job create with and for user defined object.
 * MockHabitObject is used as the user defined object.
 * 
 * Sequence:
 * -Read data from a DarwinCore extension file
 * -Map it to MockHabitObject
 * -Stream the MockHabitObject on the Java Messaging System (JMS)
 * -Create a node(mocked) that would receive the message containing the MockHabitObject
 * -Rebuild the MockHabitObject from the received message
 * -Write the object
 * @author canadensys
 *
 */
public class UserDefinedJobTest implements FutureCallback<Void>{
	
	private static final String TEST_BROKER_URL = "vm://localhost?broker.persistent=false";
	private static AtomicBoolean jobComplete = new AtomicBoolean(false);
	
	//Job initiator related variables
	private InnerUserDefinedJob userDefinedJob;
	
	private DwcaExtensionReader<MockHabitObject> itemReader;
	private GenericStreamStep<MockHabitObject> streamStep;
	private JMSWriter jmsWriter;
	
	//Node related variables
	private GenericAsyncStep<MockHabitObject> asyncStep;
	private JMSConsumer jmsReader;
	private MockObjectWriter<MockHabitObject> itemWriter;
	
	@Test
	public void testEntireLoop(){
		
		//1- Build a item reader
		itemReader = new DwcaExtensionReader<MockHabitObject>();
		
		//2- Build a mapper to control how DarwinCore properties are mapped to user defined object
		ItemMapperIF<MockHabitObject> itemMapper = new MockHabitMapper();
		
		//3- Link the reader with our mapper
		itemReader.setMapper(itemMapper);
		
		//4-Create a JMS writer
		jmsWriter = new JMSWriter(TEST_BROKER_URL);
		
		//5- Declare the message handler classes. Which class(es) on the node(s) should handle the messages.
		List<Class<? extends JMSConsumerMessageHandler>> msgHandlerClassList = new ArrayList<Class<? extends JMSConsumerMessageHandler>>();
		msgHandlerClassList.add(GenericAsyncStep.class);
		
		//6- Create and configure the stream step
		streamStep = new GenericStreamStep<MockHabitObject>();
		streamStep.setReader(itemReader);
		streamStep.setWriter(jmsWriter);
		streamStep.setMessageClasses(msgHandlerClassList);

		//7- Create our job
		userDefinedJob = new InnerUserDefinedJob();
		
		//8- Add our step
		userDefinedJob.setGenericStreamStep(streamStep);
		
		//add a local consumer to test the entire loop
		setupTestConsumer(2);
						
		userDefinedJob.addToSharedParameters(SharedParameterEnum.DWCA_PATH,"src/test/resources/dwca-vascan-checklist");
		userDefinedJob.addToSharedParameters(SharedParameterEnum.DWCA_EXTENSION_TYPE,"description");

		userDefinedJob.doJob();
		synchronized (jobComplete) {
			try {
				jobComplete.wait();
				//validate content of the database
				if(jobComplete.get()){
					List<MockHabitObject> objectWritten = itemWriter.getContent();
					assertEquals("1941",objectWritten.get(0).getId());
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
	 * This consumer will write using a MockObjectWriter so we can get the written object back.
	 */
	private void setupTestConsumer(int numberOfRecord){
		jmsReader = new JMSConsumer(TEST_BROKER_URL);
		
		//1- Create our async step that would run on a node
		asyncStep = new GenericAsyncStep<MockHabitObject>(MockHabitObject.class);
		
		//2- Create and set our writer
		itemWriter = new MockObjectWriter<MockHabitObject>();
		//This callback mechanism is for testing purpose only
		itemWriter.addCallback(this, numberOfRecord);
		asyncStep.setWriter(itemWriter);
		
		//3- Register our step as an handler (JMSConsumerMessageHandler)
		jmsReader.registerHandler(asyncStep);
		
		try {
			((ProcessingStepIF)asyncStep).preStep(null);
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
		
		//4- Start listening
		jmsReader.open();
	}

	@Override
	public void onSuccess(Void result) {
		synchronized (jobComplete) {
			jobComplete.set(true);
			jobComplete.notifyAll();
		}
	}

	@Override
	public void onFailure(Throwable t) {
		fail();
	}
	
	/**
	 * User defined job declared as inner class.
	 * @author canadensys
	 *
	 */
	private class InnerUserDefinedJob extends AbstractProcessingJob{
		private ProcessingStepIF genericStreamStep;
		
		public InnerUserDefinedJob(){
			sharedParameters = new HashMap<SharedParameterEnum, Object>();
		}
		
		public void setGenericStreamStep(ProcessingStepIF genericStreamStep) {
			this.genericStreamStep = genericStreamStep;
		}
		
		public void doJob(){
			executeStepSequentially(genericStreamStep, sharedParameters);
		}
	}

}
