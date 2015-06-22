package net.canadensys;

import java.util.Scanner;

import net.canadensys.harvester.main.MigrationMain;
import net.canadensys.harvester.main.MigrationMain.Mode;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * Entry point for lontra-cli
 * 
 * @author cgendreau
 *
 */
public class Main {

	private static Options cmdLineOptions;
	private static final String MIGRATE_SHORT_OPTION = "m";
	private static final String MIGRATE_OPTION = "migrate";

	private static final String MIGRATE_OPTION_DRYRUN = "dryrun";
	private static final String MIGRATE_OPTION_APPLY = "apply";

	static {
		cmdLineOptions = new Options();
		cmdLineOptions.addOption(new Option(MIGRATE_SHORT_OPTION, MIGRATE_OPTION, true, "Migrate database '" + MIGRATE_OPTION_DRYRUN + "' or '"
				+ MIGRATE_OPTION_APPLY + "'"));
	}

	/**
	 * Harvester CLI entry point
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// cmdLineOptions.addOption("r", true, "sourcefileid to harvest");

		CommandLineParser parser = new PosixParser();
		CommandLine cmdLine = null;
		try {
			cmdLine = parser.parse(cmdLineOptions, args);
		}
		catch (ParseException e) {
			System.out.println(e.getMessage());
		}

		if (cmdLine != null) {
			// handle migration
			if (cmdLine.hasOption(MIGRATE_OPTION)) {
				String optionValue = cmdLine.getOptionValue(MIGRATE_OPTION);
				if (MIGRATE_OPTION_DRYRUN.equalsIgnoreCase(optionValue)) {
					MigrationMain.main(Mode.DRYRUN);
				}
				else if (MIGRATE_OPTION_APPLY.equalsIgnoreCase(optionValue)) {
					Scanner sc = new Scanner(System.in);
					// ask confirmation
					System.out.println("Are you sure you want to apply database migration? (yes/no)");

					if ("yes".equalsIgnoreCase(sc.nextLine())) {
						MigrationMain.main(Mode.APPLY);
					}
					sc.close();
				}
				else {
					printHelp();
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
