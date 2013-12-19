package net.canadensys.processing.occurrence.mock.processor;

import java.util.Map;

import net.canadensys.processing.ItemProcessorIF;
import net.canadensys.processing.exception.ProcessException;
import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.mock.MockHabitObject;
import net.canadensys.processing.occurrence.mock.MockProcessedHabitObject;

/**
 * Simple processor that will take the id in MockHabitObject and parse it as an int and return a MockProcessedHabitObject
 * @author canadensys
 *
 */
public class MockHabitProcessor implements ItemProcessorIF<MockHabitObject, MockProcessedHabitObject>{

	@Override
	public void init() {
		
	}

	@Override
	public MockProcessedHabitObject process(MockHabitObject data,
			Map<SharedParameterEnum, Object> sharedParameters)
			throws ProcessException {
		MockProcessedHabitObject processedObj = new MockProcessedHabitObject();
		processedObj.setId(Integer.parseInt(data.getId()));
		processedObj.setDescription(data.getDescription());
		return processedObj;
	}

	@Override
	public void destroy() {
	}

}
