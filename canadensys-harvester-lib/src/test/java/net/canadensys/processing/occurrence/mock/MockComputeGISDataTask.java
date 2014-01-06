package net.canadensys.processing.occurrence.mock;

import java.util.Map;

import net.canadensys.processing.occurrence.SharedParameterEnum;
import net.canadensys.processing.occurrence.task.ComputeGISDataTask;

public class MockComputeGISDataTask extends ComputeGISDataTask{
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) {
		System.out.println("Using mock");
		return;
	}
}
