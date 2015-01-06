package net.canadensys.harvester.occurrence.mock.processor;

import java.util.Map;

import net.canadensys.harvester.ItemProcessorIF;
import net.canadensys.harvester.exception.ProcessException;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.mock.MockHabitObject;
import net.canadensys.harvester.occurrence.mock.MockProcessedHabitObject;

/**
 * Simple processor that will take the id in MockHabitObject and parse it as an int and return a MockProcessedHabitObject
 * 
 * @author canadensys
 * 
 */
public class MockHabitProcessor implements ItemProcessorIF<MockHabitObject, MockProcessedHabitObject> {

	@Override
	public void init() {

	}

	@Override
	public MockProcessedHabitObject process(MockHabitObject data, Map<SharedParameterEnum, Object> sharedParameters) throws ProcessException {
		MockProcessedHabitObject processedObj = new MockProcessedHabitObject();
		processedObj.setId(Integer.parseInt(data.getId()));
		processedObj.setDescription(data.getDescription());
		return processedObj;
	}

	@Override
	public void destroy() {
	}

}
