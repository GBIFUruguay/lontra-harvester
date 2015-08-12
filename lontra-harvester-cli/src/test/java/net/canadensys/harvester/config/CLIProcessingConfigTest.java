package net.canadensys.harvester.config;

import net.canadensys.harvester.AbstractProcessingJob;
import net.canadensys.harvester.CLIService;
import net.canadensys.harvester.MockCLIService;
import net.canadensys.harvester.MockJob;
import net.canadensys.harvester.MockResourceStatusChecker;
import net.canadensys.harvester.main.JobInitiatorMain;
import net.canadensys.harvester.occurrence.status.ResourceStatusCheckerIF;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class using Spring annotations.
 * All the beans that could be changed based on configuration or could be mock are created from here.
 * 
 * @author canadensys
 * 
 */
@Configuration
public class CLIProcessingConfigTest {

	@Bean
	public JobInitiatorMain jobInitiatorMain() {
		return new JobInitiatorMain();
	}

	@Bean
	public CLIService jobService() {
		return new MockCLIService();
	}

	@Bean
	public ResourceStatusCheckerIF resourceStatusNotifierIF() {
		return new MockResourceStatusChecker();
	}

	// ---JOB---
	@Bean
	public AbstractProcessingJob importDwcaJob() {
		return new MockJob();
	}

	@Bean
	public AbstractProcessingJob moveToPublicSchemaJob() {
		return new MockJob();
	}

	@Bean
	public AbstractProcessingJob computeUniqueValueJob() {
		return new MockJob();
	}

}
