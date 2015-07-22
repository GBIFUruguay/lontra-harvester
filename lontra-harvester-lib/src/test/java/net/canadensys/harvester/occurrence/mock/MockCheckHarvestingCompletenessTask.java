package net.canadensys.harvester.occurrence.mock;

import java.util.Map;

import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.task.CheckHarvestingCompletenessTask;

public class MockCheckHarvestingCompletenessTask extends CheckHarvestingCompletenessTask {

	@Override
	public void execute(Map<SharedParameterEnum, Object> sharedParameters) {
		notifyListenersOnSuccess();
	}

}
