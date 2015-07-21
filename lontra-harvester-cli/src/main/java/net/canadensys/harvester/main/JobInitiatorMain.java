package net.canadensys.harvester.main;

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
		RESOURCE_STATUS, LIST_RESOURCE, IMPORT_DWCA
	}

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

	public static void jobMain(JobType jobType) {
		jobMain(jobType, null);
	}

	/**
	 * JobInitiator Entry point
	 * 
	 * @param jobType
	 * @param args
	 */
	public static void jobMain(JobType jobType, String arg) {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(CLIProcessingConfig.class);
		JobInitiatorMain jim = ctx.getBean(JobInitiatorMain.class);

		switch (jobType) {
			case RESOURCE_STATUS:
				jim.displayResourceStatus();
				break;
			case LIST_RESOURCE:
				jim.displayResourceList();
				break;
			case IMPORT_DWCA:
				jim.importDwca(arg);
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

	private void importDwca(String resourceIdentifier) {
		DwcaResourceModel resourceModel = cliService.loadResourceModel(resourceIdentifier);
		System.out.println("would load " + resourceModel.getName());
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

}
