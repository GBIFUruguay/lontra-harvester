package net.canadensys;

import java.util.Scanner;

import net.canadensys.harvester.main.JobInitiatorMain;
import net.canadensys.harvester.main.MigrationMain;
import net.canadensys.harvester.main.MigrationMain.Mode;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Entry point for lontra-cli
 * 
 * @author cgendreau
 *
 */
public class Main {

	private static Options cmdLineOptions;

	private static final String CONFIG_SHORT_OPTION = "c";
	private static final String CONFIG_OPTION = "config";

	private static final String STATUS_SHORT_OPTION = "s";
	private static final String STATUS_OPTION = "status";

	private static final String RESOURCE_LIST_SHORT_OPTION = "l";
	private static final String RESOURCE_LIST_OPTION = "list";

	private static final String HARVEST_SHORT_OPTION = "h";
	private static final String HARVEST_OPTION = "harvest";
	private static final String NO_NODES_SHORT_OPTION = "N";
	private static final String NO_NODES_OPTION = "nonodes";

	// migration related options
	private static final String MIGRATE_SHORT_OPTION = "m";
	private static final String MIGRATE_OPTION = "migrate";
	private static final String MIGRATE_OPTION_DRYRUN = "dryrun";
	private static final String MIGRATE_OPTION_APPLY = "apply";

	static {
		cmdLineOptions = new Options();
		cmdLineOptions.addOption(new Option(CONFIG_SHORT_OPTION, CONFIG_OPTION, false, "Location of configuration file"));
		cmdLineOptions.addOption(new Option(MIGRATE_SHORT_OPTION, MIGRATE_OPTION, true, "Migrate database '" + MIGRATE_OPTION_DRYRUN + "' or '"
				+ MIGRATE_OPTION_APPLY + "'"));
		cmdLineOptions.addOption(new Option(RESOURCE_LIST_SHORT_OPTION, RESOURCE_LIST_OPTION, false, "List all resources"));
		cmdLineOptions.addOption(new Option(STATUS_SHORT_OPTION, STATUS_OPTION, false, "List status of resources"));
		cmdLineOptions.addOption(Option.builder(HARVEST_SHORT_OPTION).longOpt(HARVEST_OPTION)
				.desc("Harvest a specific resource or a resource that requires to be harvested if no <resource idenfifier> is provided")
				.argName("resource idenfifier").hasArg()
				.optionalArg(true)
				.build());
		cmdLineOptions.addOption(new Option(NO_NODES_SHORT_OPTION, NO_NODES_OPTION, false, "Harvest using no nodes"));
	}

	/**
	 * Harvester CLI entry point
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		CommandLineParser parser = new DefaultParser();
		CommandLine cmdLine = null;
		try {
			cmdLine = parser.parse(cmdLineOptions, args);
		}
		catch (ParseException e) {
			System.out.println(e.getMessage());
		}

		if (cmdLine != null) {

			String configOptionValue = cmdLine.getOptionValue(CONFIG_OPTION);

			// handle migration
			if (cmdLine.hasOption(MIGRATE_OPTION)) {
				String optionValue = cmdLine.getOptionValue(MIGRATE_OPTION);
				if (MIGRATE_OPTION_DRYRUN.equalsIgnoreCase(optionValue)) {
					MigrationMain.main(Mode.DRYRUN, configOptionValue);
				}
				else if (MIGRATE_OPTION_APPLY.equalsIgnoreCase(optionValue)) {
					Scanner sc = new Scanner(System.in);
					// ask confirmation
					System.out.println("Are you sure you want to apply database migration? (yes/no)");

					if ("yes".equalsIgnoreCase(sc.nextLine())) {
						MigrationMain.main(Mode.APPLY, configOptionValue);
					}
					sc.close();
				}
				else {
					printHelp();
				}
			}
			else if (cmdLine.hasOption(RESOURCE_LIST_OPTION)) {
				JobInitiatorMain.jobMain(JobInitiatorMain.JobType.LIST_RESOURCE);
			}
			else if (cmdLine.hasOption(STATUS_OPTION)) {
				JobInitiatorMain.jobMain(JobInitiatorMain.JobType.RESOURCE_STATUS);
			}
			else if (cmdLine.hasOption(HARVEST_OPTION)) {
				String optionValue = cmdLine.getOptionValue(HARVEST_OPTION);
				boolean noNodes = cmdLine.hasOption(NO_NODES_OPTION);

				if (noNodes) {
					System.out.println("harvest " + optionValue + " with no nodes");
				}
				else {
					JobInitiatorMain.jobMain(JobInitiatorMain.JobType.HARVEST, optionValue);
				}
			}
			else {
				printHelp();
			}
		}
	}

	/**
	 * Print the "usage" to the standard output.
	 */
	public static void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("lontra-harvester-cli", cmdLineOptions);
	}
}
