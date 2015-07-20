package net.canadensys.harvester.main;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.CLIService;
import net.canadensys.harvester.config.CLIProcessingConfig;
import net.canadensys.harvester.occurrence.model.DwcaResourceStatusModel;
import net.canadensys.harvester.occurrence.status.ResourceStatusCheckerIF;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class JobInitiatorMain {

	public enum JobType {
		RESOURCE_STATUS, LIST_RESOURCE
	}

	// @Autowired
	// private ImportDwcaJob importDwcaJob;
	//
	// @Autowired
	// private MoveToPublicSchemaJob moveToPublicSchemaJob;
	//
	// @Autowired
	// private ComputeUniqueValueJob computeUniqueValueJob;

	@Autowired
	private CLIService cliService;

	@Autowired
	private ResourceStatusCheckerIF resourceStatusNotifier;

	/**
	 * JobInitiator Entry point
	 * 
	 * @param args
	 */
	// public static void main(String sourcefileid) {
	// AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(CLIProcessingConfig.class);
	// JobInitiatorMain jim = ctx.getBean(JobInitiatorMain.class);
	// jim.initiateApp(sourcefileid);
	// }

	/**
	 * JobInitiator Entry point
	 * 
	 * @param args
	 */
	public static void startStatusMain(JobType jobType) {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(CLIProcessingConfig.class);
		JobInitiatorMain jim = ctx.getBean(JobInitiatorMain.class);

		switch (jobType) {
			case RESOURCE_STATUS:
				jim.displayResourceStatus();
				break;
			case LIST_RESOURCE:
				jim.displayResourceList();
				break;
			default:
				break;
		}
	}

	private void displayResourceStatus() {
		List<DwcaResourceStatusModel> harvestRequiredList = resourceStatusNotifier.getHarvestRequiredList();
		DwcaResourceModel resource;
		for (DwcaResourceStatusModel resourceStatus : harvestRequiredList) {
			resource = resourceStatus.getDwcaResourceModel();
			System.out.println("[" + resource.getId() + "] " + resource.getName() +
					"=> lastHarvest: " + getDateAsString(resourceStatus.getLastHarvestDate()) +
					", lastPublication: " + getDateAsString(resourceStatus.getLastPublishedDate()));
		}
	}

	private void displayResourceList() {
		List<DwcaResourceModel> resourceList = cliService.loadResourceModelList();
		for (DwcaResourceModel resource : resourceList) {
			System.out.println("[" + resource.getId() + "] " + resource.getName());
		}
	}

	/**
	 * Get Date object as String utility function for command line display.
	 * 
	 * @param date
	 * @return the date as String or "?" in case the date is null
	 */
	private String getDateAsString(Date date) {
		if (date == null) {
			return "?";
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
		return sdf.format(date);
	}

	// public void initiateApp(final String sourcefileid) {
	//
	// DwcaResourceModel resourceModel = jobService.loadResourceModel(sourcefileid);
	//
	// if (resourceModel != null) {
	//
	// ExecutorService executor = Executors.newFixedThreadPool(2);
	// importDwcaJob.addToSharedParameters(SharedParameterEnum.RESOURCE_ID, resourceModel.getId());
	// final JobStatusModel jobStatusModel = new JobStatusModel();
	// jobStatusModel.addPropertyChangeListener(new JobStatusModelListener());
	//
	// Runnable importJobThread = new Runnable() {
	// @Override
	// public void run() {
	// importDwcaJob.doJob(jobStatusModel);
	// }
	// };
	//
	// executor.execute(importJobThread);
	// executor.shutdown();
	// try {
	// executor.awaitTermination(12, TimeUnit.HOURS);
	// }
	// catch (InterruptedException e) {
	// e.printStackTrace();
	// }
	//
	// System.out.println("done");
	// }
	// else {
	// System.out.println("Can't find the resource named [" + sourcefileid + "]");
	// }
	//
	// // moveToPublicSchemaJob.addToSharedParameters(SharedParameterEnum.DATASET_SHORTNAME, datasetShortName);
	// // JobStatusModel jobStatusModel = new JobStatusModel();
	// // moveToPublicSchemaJob.doJob(jobStatusModel);
	// //
	// // computeUniqueValueJob.doJob(jobStatusModel);
	// }

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
