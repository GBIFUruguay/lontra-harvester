package net.canadensys;

import java.util.Scanner;

import net.canadensys.harvester.main.MigrationMain;
import net.canadensys.harvester.main.MigrationMain.Mode;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class Main {

	private static final String MIGRATE_SHORT_OPTION = "m";
	private static final String MIGRATE_OPTION = "migrate";

	private static final String MIGRATE_OPTION_DRYRUN = "dryrun";
	private static final String MIGRATE_OPTION_APPLY = "apply";

	/**
	 * Harvester CLI entry point
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Options cmdLineOptions = new Options();
		// cmdLineOptions.addOption("r", true, "sourcefileid to harvest");
		cmdLineOptions.addOption(MIGRATE_SHORT_OPTION, MIGRATE_OPTION, true, "migrate database");

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
						// MigrationMain.main(Mode.APPLY);
						System.out.println("apply");
					}

					sc.close();
				}
			}
		}
	}
}
