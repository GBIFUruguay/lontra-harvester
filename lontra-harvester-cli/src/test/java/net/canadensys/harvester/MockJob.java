package net.canadensys.harvester;

import java.util.UUID;

import net.canadensys.harvester.occurrence.model.JobStatusModel;

public class MockJob extends AbstractProcessingJob {

	public MockJob() {
		super(UUID.randomUUID().toString());
	}

	@Override
	public void cancel() {

	}

	@Override
	public void doJob(JobStatusModel jobStatusModel) {

	}

}
