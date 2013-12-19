package net.canadensys.processing.occurrence.step.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.canadensys.processing.jms.JMSConsumerMessageHandler;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.message.DefaultMessage;
import net.canadensys.processing.occurrence.mock.MockHabitObject;
import net.canadensys.processing.occurrence.mock.mapper.MockHabitMapper;
import net.canadensys.processing.occurrence.mock.writer.MockObjectWriter;
import net.canadensys.processing.occurrence.reader.DwcaExtensionReader;
import net.canadensys.processing.occurrence.step.async.GenericAsyncStep;

import org.junit.Test;

/**
 * Unit test for GenericStreamStep
 * @author cgendreau
 *
 */
public class GenericStreamStepTest {
	
	@Test
	public void testGenericStreamStep(){
		GenericStreamStep<MockHabitObject> streamHabitStep = new GenericStreamStep<MockHabitObject>();
		MockObjectWriter<DefaultMessage> writer = new MockObjectWriter<DefaultMessage>();
		Map<SharedParameterEnum,Object> sharedParameters = new HashMap<SharedParameterEnum, Object>();
		sharedParameters.put(SharedParameterEnum.DWCA_PATH,"src/test/resources/dwca-vascan-checklist");
		sharedParameters.put(SharedParameterEnum.DWCA_EXTENSION_TYPE,"description");
		
		//setup reader
		DwcaExtensionReader<MockHabitObject> extReader = new DwcaExtensionReader<MockHabitObject>();
		extReader.setMapper(new MockHabitMapper());
		
		streamHabitStep.setReader(extReader);
		streamHabitStep.setWriter(writer);
		
		List<Class<? extends JMSConsumerMessageHandler>> msgHandlerClassList = new ArrayList<Class<? extends JMSConsumerMessageHandler>>();
		msgHandlerClassList.add(GenericAsyncStep.class);
		streamHabitStep.setMessageClasses(msgHandlerClassList);
		
		//run the step
		streamHabitStep.preStep(sharedParameters);
		streamHabitStep.doStep();
		streamHabitStep.postStep();
		
		//get the message that would have been wrote to JMS
		List<DefaultMessage> messages = writer.getContent();
		DefaultMessage firstMessage = (DefaultMessage)messages.get(0);
		
		//ensure class name are preserved
		assertEquals(GenericAsyncStep.class.getName(),firstMessage.getMsgHandlerClass().getName());
		assertEquals(MockHabitObject.class.getName(),firstMessage.getContentClass().getName());
		
		//ensure that we can cast the content into the specified class
		try{
			firstMessage.getContentClass().cast(firstMessage.getContent());
		}
		catch(ClassCastException ccEx){
			fail("Can not cast the message object into the declared class");
		}
	}
}
