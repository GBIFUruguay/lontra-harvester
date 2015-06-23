package net.canadensys.harvester.main;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.JobServiceIF;
import net.canadensys.harvester.config.CLIProcessingConfig;
import net.canadensys.harvester.occurrence.SharedParameterEnum;
import net.canadensys.harvester.occurrence.job.ComputeUniqueValueJob;
import net.canadensys.harvester.occurrence.job.ImportDwcaJob;
import net.canadensys.harvester.occurrence.job.MoveToPublicSchemaJob;
import net.canadensys.harvester.occurrence.model.JobStatusModel;
import net.canadensys.harvester.occurrence.notification.ResourceStatusNotifierIF;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class JobInitiatorMain {

	@Autowired
	private ImportDwcaJob importDwcaJob;

	@Autowired
	private MoveToPublicSchemaJob moveToPublicSchemaJob;

	@Autowired
	private ComputeUniqueValueJob computeUniqueValueJob;

	@Autowired
	private JobServiceIF jobService;

	@Autowired
	private ResourceStatusNotifierIF resourceStatusNotifier;

	/**
	 * JobInitiator Entry point
	 * 
	 * @param args
	 */
	public static void main(String sourcefileid) {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(CLIProcessingConfig.class);
		JobInitiatorMain jim = ctx.getBean(JobInitiatorMain.class);
		jim.initiateApp(sourcefileid);
	}

	/**
	 * JobInitiator Entry point
	 * 
	 * @param args
	 */
	public static void startStatusMain() {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(CLIProcessingConfig.class);
		JobInitiatorMain jim = ctx.getBean(JobInitiatorMain.class);
		jim.displayResourceStatus();
	}

	private void displayResourceStatus() {
		List<DwcaResourceModel> harvestRequiredList = resourceStatusNotifier.getHarvestRequiredList();

		for (DwcaResourceModel resource : harvestRequiredList) {
			System.out.println("[" + resource.getId() + "] " + resource.getName());
		}
	}

	public void initiateApp(final String sourcefileid) {

		DwcaResourceModel resourceModel = jobService.loadResourceModel(sourcefileid);

		if (resourceModel != null) {

			ExecutorService executor = Executors.newFixedThreadPool(2);
			importDwcaJob.addToSharedParameters(SharedParameterEnum.RESOURCE_ID, resourceModel.getId());
			final JobStatusModel jobStatusModel = new JobStatusModel();
			jobStatusModel.addPropertyChangeListener(new JobStatusModelListener());

			Runnable importJobThread = new Runnable() {
				@Override
				public void run() {
					importDwcaJob.doJob(jobStatusModel);
				}
			};

			executor.execute(importJobThread);
			executor.shutdown();
			try {
				executor.awaitTermination(12, TimeUnit.HOURS);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}

			System.out.println("done");
		}
		else {
			System.out.println("Can't find the resource named [" + sourcefileid + "]");
		}

		// moveToPublicSchemaJob.addToSharedParameters(SharedParameterEnum.DATASET_SHORTNAME, datasetShortName);
		// JobStatusModel jobStatusModel = new JobStatusModel();
		// moveToPublicSchemaJob.doJob(jobStatusModel);
		//
		// computeUniqueValueJob.doJob(jobStatusModel);
	}

	/**
	 * Simple PropertyChangeListener to send notifications about the JobStatusModel to the console.
	 * 
	 * @author cgendreau
	 * 
	 */
	private static class JobStatusModelListener implements PropertyChangeListener {
		@Override
		public void propertyChange(PropertyChangeEvent evt) {
			System.out.println(evt.getNewValue());
		}

	}
}
