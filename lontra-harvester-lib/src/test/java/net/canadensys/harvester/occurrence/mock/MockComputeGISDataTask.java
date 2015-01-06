package net.canadensys.harvester.occurrence.mock;

import java.util.Map;

import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.task.ComputeGISDataTask;

public class MockComputeGISDataTask extends ComputeGISDataTask {
	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) {
		System.out.println("Using mock");
		return;
	}
}
