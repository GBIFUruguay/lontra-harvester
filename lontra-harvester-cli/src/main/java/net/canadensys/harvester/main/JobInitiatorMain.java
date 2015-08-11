package net.canadensys.harvester.main;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import net.canadensys.dataportal.occurrence.model.DwcaResourceModel;
import net.canadensys.harvester.CLIService;
import net.canadensys.harvester.model.CliOption;
import net.canadensys.harvester.occurrence.model.DwcaResourceStatusModel;
import net.canadensys.harvester.occurrence.status.ResourceStatusCheckerIF;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Main class to start 'jobs'.
 * A job here is not necessarily a real lontra job (implementing AbstractProcessingJob class).
 * 
 * @author cgendreau
 *
 */
public class JobInitiatorMain {

	// Type of command available
	public enum CommandType {
		RESOURCE_STATUS, LIST_RESOURCE, HARVEST
	}

	@Autowired
	private CLIService cliService;

	@Autowired
	private ResourceStatusCheckerIF resourceStatusChecker;

	/**
	 * JobInitiator Entry point
	 * 
	 * @param cliOption
	 *            CLIProcessingConfig.class
	 */
	public static void jobMain(CliOption cliOption, Class<?> configClass) {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(configClass);
		JobInitiatorMain jim = ctx.getBean(JobInitiatorMain.class);

		switch (cliOption.getCommandType()) {
			case RESOURCE_STATUS:
				jim.displayResourceStatus();
				break;
			case LIST_RESOURCE:
				jim.displayResourceList();
				break;
			case HARVEST:
				if (StringUtils.isNotBlank(cliOption.getResourceIdentifier())) {
					jim.harvest(cliOption);
				}
				else {
					jim.harvestRequired();
				}
				break;
			default:
				break;
		}
	}

	private void displayResourceStatus() {
		List<DwcaResourceStatusModel> harvestRequiredList = resourceStatusChecker.getHarvestRequiredList();
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
	 * Harvest a specific resource.
	 * 
	 * @param cliOption
	 */
	private void harvest(CliOption cliOption) {
		DwcaResourceModel resourceModel = cliService.loadResourceModel(cliOption.getResourceIdentifier());
		if (resourceModel != null) {
			try {
				cliService.importDwca(resourceModel, cliOption);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			System.out.println("Can not find resource identified by " + cliOption.getResourceIdentifier());
		}
	}

	/**
	 * Harvest a resource that requires to be harvested as determined by resourceStatusChecker.
	 */
	private void harvestRequired() {
		List<DwcaResourceStatusModel> harvestRequiredList = resourceStatusChecker.getHarvestRequiredList();
		if (!harvestRequiredList.isEmpty()) {
			DwcaResourceModel resourceModel = harvestRequiredList.get(0).getDwcaResourceModel();
			try {
				cliService.importDwca(resourceModel, null);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			System.out.println("No harvest required");
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

}
